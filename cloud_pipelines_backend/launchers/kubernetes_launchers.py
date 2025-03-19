from __future__ import annotations

import copy
import logging
import os
import pathlib
import typing
from typing import Any, Optional

from kubernetes import client as k8s_client_lib

from cloud_pipelines.orchestration.launchers import naming_utils
from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_provider_interfaces,
)
from cloud_pipelines.orchestration.storage_providers import local_storage
from cloud_pipelines.orchestration.storage_providers import google_cloud_storage
from .. import component_structures as structures
from . import container_component_utils
from . import interfaces

if typing.TYPE_CHECKING:
    from google.cloud import storage

_logger = logging.getLogger(__name__)

_MAX_INPUT_VALUE_SIZE = 10000
_MAIN_CONTAINER_NAME = "main"


# Kubernetes annotation keys. (Has strict naming policy. Single slash only etc.)
_CLOUD_PIPELINES_KUBERNETES_ANNOTATION_KEY = "cloud-pipelines.net"
_KUBERNETES_LAUNCHER_ANNOTATION_KEY = "cloud-pipelines.net/launchers.kubernetes"
# ComponentSpec annotation keys
POD_PATCH_ANNOTATION_KEY = "cloud-pipelines.net/launchers/kubernetes/pod_patch"
MAIN_CONTAINER_PATCH_ANNOTATION_KEY = (
    "cloud-pipelines.net/launchers/kubernetes/main_container_patch"
)

_T = typing.TypeVar("_T")

_CONTAINER_FILE_NAME = "data"


def _create_volume_and_volume_mount_host_path(
    container_path: str,
    artifact_uri: str,
    suggested_volume_name: str,
    read_only: bool | None = None,
) -> tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount]:
    container_dir, _, container_file = container_path.rpartition("/")
    artifact_dir_uri, _, artifact_file = artifact_uri.rpartition("/")
    if container_file != artifact_file:
        raise interfaces.LauncherError(
            "Container file name is different from artifact file name. {container_path=}, {artifact_uri=}"
        )
    # artifact_dir_uri = pathlib.PurePosixPath(artifact_uri).parent.as_posix()
    host_dir = artifact_dir_uri
    sub_path = ""
    # host_dir + "/" + sub_path == artifact_dir
    if os.name == "nt":
        host_dir = windows_path_to_docker_path(host_dir)
    return (
        k8s_client_lib.V1Volume(
            name=suggested_volume_name,
            host_path=k8s_client_lib.V1HostPathVolumeSource(
                path=host_dir,
                # type=?
            ),
        ),
        k8s_client_lib.V1VolumeMount(
            name=suggested_volume_name,
            mount_path=container_dir,
            read_only=read_only,
            sub_path=sub_path,
        ),
    )


def _create_volume_and_volume_mount_google_cloud_storage(
    container_path: str,
    artifact_uri: str,
    suggested_volume_name: str,
    read_only: bool | None = None,
) -> tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount]:
    container_dir, _, container_file = container_path.rpartition("/")
    artifact_dir_uri, _, artifact_file = artifact_uri.rpartition("/")
    if container_file != artifact_file:
        raise interfaces.LauncherError(
            "Container file name is different from artifact file name. {container_path=}, {artifact_uri=}"
        )

    bucket_name, _, sub_path = artifact_dir_uri.removeprefix("gs://").partition("/")
    volume_name = f"gcsfuse-{bucket_name}"

    return (
        k8s_client_lib.V1Volume(
            name=volume_name,
            csi=k8s_client_lib.V1CSIVolumeSource(
                driver="gcsfuse.csi.storage.gke.io",
                volume_attributes={
                    "bucketName": bucket_name,
                    "mountOptions": "implicit-dirs",
                },
            ),
        ),
        k8s_client_lib.V1VolumeMount(
            name=volume_name,
            mount_path=container_dir,
            read_only=read_only,
            sub_path=sub_path,
        ),
    )


class _KubernetesContainerLauncher(
    interfaces.ContainerTaskLauncher["LaunchedKubernetesContainer"]
):
    """Launcher that uses single-node Kubernetes (uses hostPath for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
        _storage_provider: storage_provider_interfaces.StorageProvider,
        _create_volume_and_volume_mount: typing.Callable[
            [str, str, str, bool],
            tuple[k8s_client_lib.V1Volume, k8s_client_lib.V1VolumeMount],
        ],
    ):
        self._namespace = namespace
        self._service_account_name = service_account_name
        self._api_client = api_client
        self._storage_provider = _storage_provider
        self._request_timeout = request_timeout
        self._pod_name_prefix = pod_name_prefix
        self._pod_labels = pod_labels
        self._pod_annotations = {
            _CLOUD_PIPELINES_KUBERNETES_ANNOTATION_KEY: "true",
            _KUBERNETES_LAUNCHER_ANNOTATION_KEY: "true",
        } | (pod_annotations or {})
        self._create_volume_and_volume_mount = _create_volume_and_volume_mount

        try:
            k8s_client_lib.VersionApi(self._api_client).get_code()
        except Exception as ex:
            raise RuntimeError(
                "Connection to the Kubernetes cluster does not seem to be working."
                " Please make sure that `kubectl cluster-info` executes without errors."
            ) from ex

    def launch_container_task(
        self,
        *,
        component_spec: structures.ComponentSpec,
        # Input arguments may be updated with new downloaded values and new URIs of uploaded values.
        input_arguments: dict[str, interfaces.InputArgument],
        output_uris: dict[str, str],
        log_uri: str,
        annotations: dict[str, Any] | None = None,
    ) -> "LaunchedKubernetesContainer":
        if not isinstance(
            component_spec.implementation, structures.ContainerImplementation
        ):
            raise interfaces.LauncherError(
                f"Component must have container implementation. {component_spec=}"
            )
        container_spec = component_spec.implementation.container

        volume_map: dict[str, k8s_client_lib.V1Volume] = {}
        volume_mounts: list[k8s_client_lib.V1VolumeMount] = []

        container_inputs_root = pathlib.PurePosixPath("/tmp/inputs")
        container_outputs_root = pathlib.PurePosixPath("/tmp/outputs")

        # Callbacks for the command-line resolving
        # Their main purpose is to return input/output path or value.
        # They add volumes and volume mounts when needed.
        # They also upload/download artifact data when needed.
        def get_input_value(input_name: str) -> str:
            input_argument = input_arguments[input_name]
            if input_argument.is_dir:
                raise interfaces.LauncherError(
                    f"Cannot consume directory as value. {input_name=}, {input_argument=}"
                )
            if input_argument.total_size > _MAX_INPUT_VALUE_SIZE:
                raise interfaces.LauncherError(
                    f"Artifact is too big to consume as value. Consume it as file instead. {input_name=}, {input_argument=}"
                )
            value = input_argument.value
            if value is None:
                # Download artifact data
                if not input_argument.uri:
                    raise interfaces.LauncherError(
                        f"Artifact data has no value and no uri. This cannot happen. {input_name=}, {input_argument=}"
                    )
                uri_reader = self._storage_provider.make_uri(
                    input_argument.uri
                ).get_reader()
                value = uri_reader.download_as_text()
                # Updating the input_arguments with the downloaded value
                input_argument.value = value
            return value

        def get_input_path(input_name: str) -> str:
            input_argument = input_arguments[input_name]
            uri = input_argument.uri
            if not uri:
                if input_argument.value is None:
                    raise interfaces.LauncherError(
                        f"Artifact data has no value and no uri. This cannot happen. {input_name=}, {input_argument=}"
                    )
                uri_writer = self._storage_provider.make_uri(
                    input_argument.staging_uri
                ).get_writer()
                uri_writer.upload_from_text(input_argument.value)
                uri = input_argument.staging_uri
                # Updating the input_arguments with the URI of the uploaded value
                input_argument.uri = uri

            container_path = (
                container_inputs_root
                / naming_utils.sanitize_file_name(input_name)
                / _CONTAINER_FILE_NAME
            ).as_posix()
            volume_name = naming_utils.sanitize_kubernetes_resource_name(
                "inputs-" + input_name
            )
            volume, volume_mount = self._create_volume_and_volume_mount(
                container_path=container_path,
                artifact_uri=uri,
                suggested_volume_name=volume_name,
                read_only=True,
            )
            volume_map[volume.name] = volume
            volume_mounts.append(volume_mount)
            return container_path

        def get_output_path(output_name: str) -> str:
            uri = output_uris[output_name]
            container_path = (
                container_outputs_root
                / naming_utils.sanitize_file_name(output_name)
                / _CONTAINER_FILE_NAME
            ).as_posix()
            volume_name = naming_utils.sanitize_kubernetes_resource_name(
                "outputs-" + output_name
            )
            volume, volume_mount = self._create_volume_and_volume_mount(
                container_path=container_path,
                artifact_uri=uri,
                suggested_volume_name=volume_name,
            )
            volume_map[volume.name] = volume
            volume_mounts.append(volume_mount)
            return container_path

        # Resolving the command line.
        # Also indirectly populates volumes and volume_mounts.
        resolved_cmd = container_component_utils.resolve_container_command_line(
            component_spec=component_spec,
            provided_input_names=set(input_arguments.keys()),
            get_input_value=get_input_value,
            get_input_path=get_input_path,
            get_output_path=get_output_path,
        )

        container_env = [
            k8s_client_lib.V1EnvVar(name=name, value=value)
            for name, value in (container_spec.env or {}).items()
        ]
        main_container_spec = k8s_client_lib.V1Container(
            name=_MAIN_CONTAINER_NAME,
            image=container_spec.image,
            command=resolved_cmd.command,
            args=resolved_cmd.args,
            env=container_env,
            volume_mounts=volume_mounts,
        )

        annotations = annotations or {}

        # Applying the main container patch
        main_container_patch = annotations.get(MAIN_CONTAINER_PATCH_ANNOTATION_KEY)
        if main_container_patch:
            # Produces proper camelCase keys
            main_container_spec_dict = _kubernetes_serialize(main_container_spec)
            _update_dict_recursively(main_container_spec_dict, main_container_patch)
            # Properly parses camelCase keys
            main_container_spec = _kubernetes_deserialize(
                obj_dict=main_container_spec_dict, cls=k8s_client_lib.V1Container
            )

        pod_spec = k8s_client_lib.V1PodSpec(
            init_containers=[],
            containers=[
                main_container_spec,
            ],
            volumes=list(volume_map.values()),
            restart_policy="Never",
            service_account_name=self._service_account_name,
        )

        pod = k8s_client_lib.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s_client_lib.V1ObjectMeta(
                generate_name=self._pod_name_prefix,
                labels=self._pod_labels,
                annotations=self._pod_annotations,
            ),
            spec=pod_spec,
        )

        # Applying the pod patch
        pod_patch = annotations.get(POD_PATCH_ANNOTATION_KEY)
        if pod_patch:
            # Produces proper camelCase keys
            pod_dict: dict = _kubernetes_serialize(pod)
            _update_dict_recursively(pod_dict, pod_patch)
            # Properly parses camelCase keys
            pod = _kubernetes_deserialize(obj_dict=pod_dict, cls=k8s_client_lib.V1Pod)

        core_api_client = k8s_client_lib.CoreV1Api(api_client=self._api_client)
        created_pod: k8s_client_lib.V1Pod = core_api_client.create_namespaced_pod(
            namespace=self._namespace,
            body=pod,
            _request_timeout=self._request_timeout,
        )

        pod_name: str = created_pod.metadata.name
        pod_namespace: str = created_pod.metadata.namespace
        _logger.info(f"Created pod {pod_name} in namespace {pod_namespace}")

        launched_kubernetes_container = LaunchedKubernetesContainer(
            pod_name=pod_name,
            namespace=pod_namespace,
            output_uris=output_uris,
            log_uri=log_uri,
            debug_pod=created_pod,
        )
        return launched_kubernetes_container

    def get_refreshed_launched_container(
        self, launched_container: "LaunchedKubernetesContainer"
    ) -> "LaunchedKubernetesContainer":
        core_api_client = k8s_client_lib.CoreV1Api(api_client=self._api_client)
        pod: k8s_client_lib.V1Pod = core_api_client.read_namespaced_pod(
            name=launched_container._pod_name,
            namespace=launched_container._namespace,
            _request_timeout=self._request_timeout,
        )

        new_launched_container = copy.copy(launched_container)
        new_launched_container._debug_pod = pod
        return new_launched_container

    def get_refreshed_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesContainer":
        launched_container = LaunchedKubernetesContainer.from_dict(
            launched_container_dict
        )
        return self.get_refreshed_launched_container(launched_container)

    def deserialize_launched_container_from_dict(
        self, launched_container_dict: dict
    ) -> "LaunchedKubernetesContainer":
        launched_container = LaunchedKubernetesContainer.from_dict(
            launched_container_dict
        )
        return launched_container


class KubernetesWithHostPathContainerLauncher(_KubernetesContainerLauncher):
    """Launcher that uses single-node Kubernetes (uses hostPath for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
    ):
        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_name_prefix=pod_name_prefix,
            _storage_provider=local_storage.LocalStorageProvider(),
            pod_labels=pod_labels,
            pod_annotations=pod_annotations,
            _create_volume_and_volume_mount=_create_volume_and_volume_mount_host_path,
        )


class KubernetesWithGcsFuseContainerLauncher(_KubernetesContainerLauncher):
    """Launcher that uses single-node Kubernetes (uses GKE-gcsfuse driver for data passing)"""

    def __init__(
        self,
        *,
        api_client: k8s_client_lib.ApiClient,
        namespace: str = "default",
        service_account_name: str | None = None,
        request_timeout: int | tuple[int, int] = 10,
        pod_name_prefix: str = "task-pod-",
        gcs_client: "storage.Client | None" = None,
        pod_labels: dict[str, str] | None = None,
        pod_annotations: dict[str, str] | None = None,
    ):
        super().__init__(
            namespace=namespace,
            service_account_name=service_account_name,
            api_client=api_client,
            request_timeout=request_timeout,
            pod_name_prefix=pod_name_prefix,
            _storage_provider=google_cloud_storage.GoogleCloudStorageProvider(
                gcs_client
            ),
            pod_labels=pod_labels,
            pod_annotations={"gke-gcsfuse/volumes": "true"} | (pod_annotations or {}),
            _create_volume_and_volume_mount=_create_volume_and_volume_mount_google_cloud_storage,
        )


class LaunchedKubernetesContainer(
    interfaces.LaunchedContainer[_KubernetesContainerLauncher]
):

    def __init__(
        self,
        pod_name: str,
        namespace: str,
        output_uris: dict[str, str],
        log_uri: str,
        debug_pod: k8s_client_lib.V1Pod,
    ):
        self._pod_name = pod_name
        self._namespace = namespace
        self._output_uris = output_uris
        self._log_uri = log_uri
        self._debug_pod = debug_pod

    def _get_main_container_terminated_state(
        self,
    ) -> k8s_client_lib.V1ContainerStateTerminated | None:
        pod_status: k8s_client_lib.V1PodStatus = self._debug_pod.status
        if not pod_status or not pod_status.container_statuses:
            return None
        container_statuses: list[k8s_client_lib.V1ContainerStatus] = (
            pod_status.container_statuses
        )
        main_container_statuses = [
            container_status
            for container_status in container_statuses
            if container_status.name == _MAIN_CONTAINER_NAME
        ]
        if len(main_container_statuses) != 1:
            raise RuntimeError(
                f"Cannot get the main container status form the pod: {self._debug_pod}"
            )
        main_container_status = main_container_statuses[0]
        main_container_state: k8s_client_lib.V1ContainerState = (
            main_container_status.state
        )
        return main_container_state.terminated

    # @property
    # def id(self) -> str:
    #     return self.pod_name

    @property
    def status(self) -> interfaces.ContainerStatus:
        phase_str = self._debug_pod.status.phase
        if phase_str == "Pending":
            return interfaces.ContainerStatus.PENDING
        elif phase_str == "Running":
            return interfaces.ContainerStatus.RUNNING
        elif phase_str == "Succeeded":
            return interfaces.ContainerStatus.SUCCEEDED
        elif phase_str == "Failed":
            return interfaces.ContainerStatus.FAILED
        else:
            return interfaces.ContainerStatus.ERROR

    @property
    def exit_code(self) -> Optional[int]:
        main_container_terminated_state = self._get_main_container_terminated_state()
        if main_container_terminated_state is None:
            return None
        return main_container_terminated_state.exit_code

    @property
    def has_ended(self) -> bool:
        main_container_terminated_state = self._get_main_container_terminated_state()
        return main_container_terminated_state is not None

    @property
    def has_succeeded(self) -> bool:
        return self.status == interfaces.ContainerStatus.SUCCEEDED

    @property
    def has_failed(self) -> bool:
        return self.status == interfaces.ContainerStatus.FAILED

    def to_dict(self) -> dict[str, Any]:
        pod_dict = _kubernetes_serialize(self._debug_pod)
        # Removing trash
        _remove_keys_with_none_values(pod_dict)
        pod_metadata = pod_dict.get("metadata")
        if pod_metadata:
            pod_metadata.pop("managedFields", None)
        result = dict(
            pod_name=self._pod_name,
            namespace=self._namespace,
            output_uris=self._output_uris,
            log_uri=self._log_uri,
            debug_pod=pod_dict,
        )
        return result

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> LaunchedKubernetesContainer:
        debug_pod = _kubernetes_deserialize(d["debug_pod"], cls=k8s_client_lib.V1Pod)
        return LaunchedKubernetesContainer(
            pod_name=d["pod_name"],
            namespace=d["namespace"],
            output_uris=d["output_uris"],
            log_uri=d["log_uri"],
            debug_pod=debug_pod,
        )

    def get_refreshed(
        self, launcher: _KubernetesContainerLauncher
    ) -> "LaunchedKubernetesContainer":
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        pod: k8s_client_lib.V1Pod = core_api_client.read_namespaced_pod(
            name=self._pod_name,
            namespace=self._namespace,
            _request_timeout=launcher._request_timeout,
        )
        new_launched_container = copy.copy(self)
        new_launched_container._debug_pod = pod
        return new_launched_container

    def get_log(self, launcher: _KubernetesContainerLauncher) -> str:
        core_api_client = k8s_client_lib.CoreV1Api(api_client=launcher._api_client)
        return core_api_client.read_namespaced_pod_log(
            name=self._pod_name,
            namespace=self._namespace,
            container=_MAIN_CONTAINER_NAME,
            timestamps=True,
            stream="All",
            _request_timeout=launcher._request_timeout,
        )

    def upload_log(self, launcher: _KubernetesContainerLauncher):
        log = self.get_log(launcher=launcher)
        uri_writer = launcher._storage_provider.make_uri(self._log_uri).get_writer()
        uri_writer.upload_from_text(log)

    def __str__(self) -> str:
        import pprint

        return pprint.pformat(self.to_dict())


def windows_path_to_docker_path(path: str) -> str:
    if os.name != "nt":
        return path

    path_obj = pathlib.Path(path)
    if not path_obj.is_absolute():
        path_obj = path_obj.resolve()

    path_parts = list(path_obj.parts)
    # Changing the drive syntax: "C:\" -> "c"
    path_parts[0] = path_parts[0][0].lower()
    # WSL2 Docker path fix. See https://stackoverflow.com/questions/62812948/volume-mounts-not-working-kubernetes-and-wsl-2-and-docker/63524931#63524931
    posix_path = pathlib.PurePosixPath("/run/desktop/mnt/host/", *path_parts)
    return str(posix_path)


def _kubernetes_serialize(obj) -> dict[str, Any]:
    shallow_client = k8s_client_lib.ApiClient.__new__(k8s_client_lib.ApiClient)
    return shallow_client.sanitize_for_serialization(obj)


def _kubernetes_deserialize(obj_dict: dict[str, Any], cls: typing.Type[_T]) -> _T:
    shallow_client = k8s_client_lib.ApiClient.__new__(k8s_client_lib.ApiClient)
    return shallow_client._ApiClient__deserialize(obj_dict, cls)


def _update_dict_recursively(d1: dict, d2: dict):
    for k, v2 in d2.items():
        if k in d1:
            v1 = d1[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                _update_dict_recursively(v1, v2)
                continue
            # elif isinstance(v1, list) and isinstance(v2, list):
            # # Merging lists is not supported yet
        d1[k] = v2


def _remove_keys_with_none_values(d: dict):
    for k, v in list(d.items()):
        if v is None:
            del d[k]
        if isinstance(v, dict):
            _remove_keys_with_none_values(v)
