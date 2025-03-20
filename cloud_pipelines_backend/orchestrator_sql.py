import copy
import json
import datetime
import logging
import time
import typing
from typing import Any


import sqlalchemy as sql
from sqlalchemy import orm

from cloud_pipelines.orchestration.storage_providers import (
    interfaces as storage_provider_interfaces,
)
from cloud_pipelines.orchestration.launchers import naming_utils

from . import backend_types_sql as bts
from . import component_structures as structures
from .launchers import interfaces as launcher_interfaces


_logger = logging.getLogger(__name__)

_T = typing.TypeVar("_T")


class OrchestratorError(RuntimeError):
    pass


class OrchestratorService_Sql:
    def __init__(
        self,
        session_factory: typing.Callable[[], orm.Session],
        launcher: launcher_interfaces.ContainerTaskLauncher,
        storage_provider: storage_provider_interfaces.StorageProvider,
        data_root_uri: str,
        logs_root_uri: str,
        default_task_annotations: dict[str, Any] | None = None,
        sleep_seconds_between_queue_sweeps: float = 1.0,
    ):
        self._session_factory = session_factory
        self._launcher = launcher
        self._storage_provider = storage_provider
        self._data_root_uri = data_root_uri
        self._logs_root_uri = logs_root_uri
        self._default_task_annotations = default_task_annotations
        self._sleep_seconds_between_queue_sweeps = sleep_seconds_between_queue_sweeps

    def run_loop(self):
        while True:
            try:
                self.process_each_queue_once()
                time.sleep(self._sleep_seconds_between_queue_sweeps)
            except:
                _logger.exception("Error while calling `process_each_queue_once`")

    def process_each_queue_once(self):
        queue_handlers = [
            self.internal_process_queued_executions_queue,
            self.internal_process_running_executions_queue,
        ]
        for queue_handler in queue_handlers:
            try:
                with self._session_factory() as session:
                    queue_handler(session=session)
            except:
                _logger.exception(f"Error while executing {queue_handler=}")

    def internal_process_queued_executions_queue(self, session: orm.Session):
        query = (
            sql.select(bts.ExecutionNode).where(
                bts.ExecutionNode.container_execution_status.in_(
                    (
                        bts.ContainerExecutionStatus.UNINITIALIZED,
                        bts.ContainerExecutionStatus.QUEUED,
                    )
                )
            )
            # TODO: Maybe add last_processed_at
            # .order_by(bts.ExecutionNode.last_processed_at)
            .limit(1)
        )
        queued_execution = session.scalar(query)
        if queued_execution:
            _logger.info(f"Before processing {queued_execution.id=}")
            try:
                self.internal_process_one_queued_execution(
                    session=session, execution=queued_execution
                )
            except:
                _logger.exception(f"Error processing {queued_execution.id=}")
                session.rollback()
                queued_execution.container_execution_status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                session.commit()
            _logger.info(f"After processing {queued_execution.id=}")
            return True
        else:
            _logger.debug(f"No queued executions found")
            return False

    def internal_process_running_executions_queue(self, session: orm.Session):
        query = (
            sql.select(bts.ContainerExecution)
            .where(
                bts.ContainerExecution.status.in_(
                    (
                        bts.ContainerExecutionStatus.PENDING,
                        bts.ContainerExecutionStatus.RUNNING,
                    )
                )
            )
            .order_by(bts.ContainerExecution.last_processed_at)
            .limit(1)
        )
        running_container_execution = session.scalar(query)
        if running_container_execution:
            try:
                _logger.info(f"Before processing {running_container_execution.id=}")
                self.internal_process_one_running_execution(
                    session=session, container_execution=running_container_execution
                )
                _logger.info(f"After processing {running_container_execution.id=}")
            except:
                _logger.exception(f"Error processing {running_container_execution.id=}")
                session.rollback()
                running_container_execution.status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                # Doing an intermediate commit here because it's most important to mark the problematic execution as SYSTEM_ERROR.
                session.commit()
                # Mark our ExecutionNode as SYSTEM_ERROR
                execution_nodes = running_container_execution.execution_nodes
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.SYSTEM_ERROR
                    )
                # Doing an intermediate commit here because it's most important to mark the problematic node as SYSTEM_ERROR.
                session.commit()
                # Skip downstream executions
                for execution_node in execution_nodes:
                    _mark_all_downstream_executions_as_skipped(
                        session=session, execution=execution_node
                    )
                session.commit()
            return True
        else:
            _logger.debug(f"No running container executions found")
            return False

    def internal_process_one_queued_execution(
        self, session: orm.Session, execution: bts.ExecutionNode
    ):
        task_spec = structures.TaskSpec.from_json_dict(execution.task_spec)
        component_spec = task_spec.component_ref.spec
        if component_spec is None:
            raise OrchestratorError(
                f"Missing ComponentSpec. {task_spec.component_ref=}"
            )
        implementation = component_spec.implementation
        if not implementation:
            raise OrchestratorError(f"Missing implementation. {component_spec=}")
        if implementation is None or not isinstance(
            implementation, structures.ContainerImplementation
        ):
            raise OrchestratorError(
                f"Implementation must be container. {implementation=}"
            )
        container_spec = implementation.container
        input_artifact_data = dict(
            session.execute(
                sql.select(bts.InputArtifactLink.input_name, bts.ArtifactData)
                .where(bts.InputArtifactLink.execution == execution)
                .join(bts.InputArtifactLink.artifact)
                .join(bts.ArtifactNode.artifact_data, isouter=True)
            )
            .tuples()
            .all()
        )
        if None in input_artifact_data.values():
            _logger.info(
                f"Execution did not have all input artifact data present. Waiting for upstream. {execution.id=}"
            )
            execution.container_execution_status = (
                bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
            )
            session.commit()
            return

        cache_key = _calculate_container_execution_cache_key(
            container_spec=container_spec, input_artifact_data=input_artifact_data
        )
        # TODO: !!! Cache reuse

        # Launching the container execution

        pipeline_run = session.scalar(
            sql.select(bts.PipelineRun)
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == bts.PipelineRun.root_execution_id,
            )
            .where(bts.ExecutionToAncestorExecutionLink.execution_id == execution.id)
        )
        session.rollback()

        if pipeline_run is None:
            raise OrchestratorError(
                f"Execution {execution} is not associated with a PipelineRun."
            )

        container_execution_uuid = _generate_random_id()

        _ARTIFACT_PATH_LAST_PART = "data"
        _LOG_FILE_NAME = "log.txt"
        # _TIMESTAMPED_LOG_FILE_NAME = "timestamped.log.txt"
        # _STDOUT_LOG_FILE_NAME = "stdout.log.txt"
        # _STDERR_LOG_FILE_NAME = "stderr.log.txt"

        # Input URIs might be needed to write constant input data.
        # Or to prevent the data from being purged during the execution.
        # TODO: Ensure unique artifact URIs (they can become non-unique due to sanitization)
        def generate_input_artifact_uri(
            root_dir: str,
            execution_id: str,
            input_name: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            input_name_str = naming_utils.sanitize_file_name(input_name)
            return f"{root_dir}/by_execution/{execution_id_str}/inputs/{input_name_str}/{_ARTIFACT_PATH_LAST_PART}"

        def generate_output_artifact_uri(
            root_dir: str,
            execution_id: str,
            output_name: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            output_name_str = naming_utils.sanitize_file_name(output_name)
            return f"{root_dir}/by_execution/{execution_id_str}/outputs/{output_name_str}/{_ARTIFACT_PATH_LAST_PART}"

        def generate_execution_log_uri(
            root_dir: str,
            execution_id: str,
        ) -> str:
            root_dir = root_dir.rstrip("/")
            execution_id_str = naming_utils.sanitize_file_name(execution_id)
            return f"{root_dir}/by_execution/{execution_id_str}/{_LOG_FILE_NAME}"

        log_uri = generate_execution_log_uri(
            root_dir=self._logs_root_uri, execution_id=container_execution_uuid
        )
        output_artifact_uris = {
            output_spec.name: generate_output_artifact_uri(
                root_dir=self._data_root_uri,
                execution_id=container_execution_uuid,
                output_name=output_spec.name,
            )
            for output_spec in component_spec.outputs or []
        }

        input_arguments = {
            input_name: launcher_interfaces.InputArgument(
                total_size=artifact_data.total_size,
                is_dir=artifact_data.is_dir,
                value=artifact_data.value,
                uri=artifact_data.uri,
                staging_uri=generate_input_artifact_uri(
                    root_dir=self._data_root_uri,
                    execution_id=container_execution_uuid,
                    input_name=input_name,
                ),
            )
            for input_name, artifact_data in input_artifact_data.items()
        }

        # Handling annotations

        full_annotations = {}
        _update_dict_recursive(
            full_annotations, copy.deepcopy(self._default_task_annotations or {})
        )
        _update_dict_recursive(
            full_annotations, copy.deepcopy(pipeline_run.annotations or {})
        )
        _update_dict_recursive(
            full_annotations, copy.deepcopy(task_spec.annotations or {})
        )

        try:
            launched_container: launcher_interfaces.LaunchedContainer = (
                self._launcher.launch_container_task(
                    component_spec=component_spec,
                    # The value and uri might be updated during launching.
                    input_arguments=input_arguments,
                    output_uris=output_artifact_uris,
                    log_uri=log_uri,
                    annotations=full_annotations,
                )
            )
            if launched_container.status not in (
                bts.ContainerExecutionStatus.PENDING,
                bts.ContainerExecutionStatus.RUNNING,
            ):
                raise OrchestratorError(
                    f"Unexpected status of just launched container: {launched_container.status=}, {launched_container=}"
                )
        except:
            session.rollback()
            with session.begin():
                # Logs whole exception
                _logger.exception(f"Error launching container for {execution.id=}")
                execution.container_execution_status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
                _mark_all_downstream_executions_as_skipped(
                    session=session, execution=execution
                )

        current_time = _get_current_time()

        container_execution = bts.ContainerExecution(
            status=bts.ContainerExecutionStatus.PENDING,
            last_processed_at=current_time,
            created_at=current_time,
            launcher_data=launched_container.to_dict(),
            # Updating the map with uploaded value URIs and downloaded values.
            input_artifact_data_map={
                input_name: dict(
                    id=artifact_data.id,
                    is_dir=artifact_data.is_dir,
                    total_size=artifact_data.total_size,
                    hash=artifact_data.hash,
                    # The value and URI might have been updated during launching.
                    value=input_arguments[input_name].value,
                    uri=input_arguments[input_name].uri,
                )
                for input_name, artifact_data in input_artifact_data.items()
            },
            output_artifact_data_map={
                output_name: dict(
                    uri=uri,
                )
                for output_name, uri in output_artifact_uris.items()
            },
            log_uri=log_uri,
        )

        # Need to commit/rollback before explicitly beginning non-nested transaction.
        session.rollback()
        with session.begin():
            session.add(container_execution)
            execution.container_execution = container_execution
            execution.container_execution_cache_key = cache_key
            execution.container_execution_status = bts.ContainerExecutionStatus.PENDING
            # TODO: Maybe add artifact value and URI to input ArtifactData.

    def internal_process_one_running_execution(
        self, session: orm.Session, container_execution: bts.ContainerExecution
    ):
        # Should we update last_processed_at early? Should we do it always (even when there are errors).
        # ALways updating the last_processed_at such that a problematic container does not get queue stuck.
        session.expire_on_commit = False
        session.rollback()
        with session.begin():
            container_execution.last_processed_at = _get_current_time()
        launcher_data = _assert_not_none(container_execution.launcher_data)
        launched_container: launcher_interfaces.LaunchedContainer = (
            self._launcher.deserialize_launched_container_from_dict(launcher_data)
        )
        previous_status = launched_container.status
        if previous_status not in (
            bts.ContainerExecutionStatus.PENDING,
            bts.ContainerExecutionStatus.RUNNING,
        ):
            raise OrchestratorError(
                f"Unexpected running container status: {previous_status=}, {launched_container=}"
            )
        reloaded_launched_container: launcher_interfaces.LaunchedContainer = (
            self._launcher.get_refreshed_launched_container_from_dict(launcher_data)
        )
        current_time = _get_current_time()
        # Saving the updated launcher data
        reloaded_launcher_data = reloaded_launched_container.to_dict()
        if reloaded_launcher_data != launcher_data:
            session.rollback()
            with session.begin():
                container_execution.launcher_data = reloaded_launcher_data
                container_execution.updated_at = current_time
        new_status: launcher_interfaces.ContainerStatus = (
            reloaded_launched_container.status
        )
        if new_status == previous_status:
            _logger.info(
                f"Container execution {container_execution.id} remains in {new_status} state."
            )
            return
        _logger.info(
            f"Container execution {container_execution.id} is now in state {new_status} (was {previous_status})."
        )
        session.rollback()
        try:
            container_execution.updated_at = current_time
            execution_nodes = container_execution.execution_nodes
            if not execution_nodes:
                raise OrchestratorError(
                    f"Could not find ExecutionNode associated with ContainerExecution. {container_execution=}"
                )
            if len(execution_nodes) > 1:
                _logger.warning(
                    f"ContainerExecution is associated with multiple ExecutionNodes: {container_execution=}, {execution_nodes=}"
                )

            if new_status == launcher_interfaces.ContainerStatus.RUNNING:
                container_execution.status = bts.ContainerExecutionStatus.RUNNING
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.RUNNING
                    )
            elif new_status == launcher_interfaces.ContainerStatus.SUCCEEDED:
                _retry(
                    lambda: reloaded_launched_container.upload_log(
                        launcher=self._launcher
                    )
                )
                _MAX_PRELOAD_VALUE_SIZE = 255

                def _maybe_preload_value(
                    uri_reader: storage_provider_interfaces.UriReader,
                    data_info: storage_provider_interfaces.DataInfo,
                ) -> str | None:
                    """Preloads artifact value is it's small enough (e.g. <=255 bytes)"""
                    if (
                        not data_info.is_dir
                        and data_info.total_size < _MAX_PRELOAD_VALUE_SIZE
                    ):
                        data = uri_reader.download_as_bytes()
                        try:
                            text = data.decode("utf-8")
                            return text
                        except:
                            pass

                output_artifact_uris: dict[str, str] = {
                    output_name: output_artifact_info_dict["uri"]
                    for output_name, output_artifact_info_dict in container_execution.output_artifact_data_map.items()
                }
                output_artifact_data_info_map = {
                    output_name: _retry(
                        lambda: self._storage_provider.make_uri(uri)
                        .get_reader()
                        .get_info()
                    )
                    for output_name, uri in output_artifact_uris.items()
                }
                new_output_artifact_data_map = {
                    output_name: bts.ArtifactData(
                        total_size=data_info.total_size,
                        is_dir=data_info.is_dir,
                        hash="md5=" + data_info.hashes["md5"],
                        uri=output_artifact_uris[output_name],
                        # Preloading artifact value is it's small enough (e.g. <=255 bytes)
                        value=_retry(
                            lambda: _maybe_preload_value(
                                uri_reader=self._storage_provider.make_uri(
                                    output_artifact_uris[output_name]
                                ).get_reader(),
                                data_info=data_info,
                            )
                        ),
                        created_at=current_time,
                    )
                    for output_name, data_info in output_artifact_data_info_map.items()
                }

                session.add_all(new_output_artifact_data_map.values())
                container_execution.status = bts.ContainerExecutionStatus.SUCCEEDED
                container_execution.exit_code = reloaded_launched_container.exit_code
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.SUCCEEDED
                    )
                    # TODO: Optimize
                    for output_name, artifact_node in session.execute(
                        sql.select(bts.OutputArtifactLink.output_name, bts.ArtifactNode)
                        .join(bts.OutputArtifactLink.artifact)
                        .where(bts.OutputArtifactLink.execution_id == execution_node.id)
                    ).tuples():
                        artifact_node.artifact_data = new_output_artifact_data_map[
                            output_name
                        ]
                        artifact_node.had_data_in_past = True
                    # Waking up all direct downstream executions.
                    # Many of them will get processed and go back to WAITING_FOR_UPSTREAM state.
                    for downstream_execution in _get_direct_downstream_executions(
                        session=session, execution=execution_node
                    ):
                        if (
                            downstream_execution.container_execution_status
                            == bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
                        ):
                            downstream_execution.container_execution_status = (
                                bts.ContainerExecutionStatus.QUEUED
                            )
            elif new_status == launcher_interfaces.ContainerStatus.FAILED:
                container_execution.status = bts.ContainerExecutionStatus.FAILED
                container_execution.exit_code = reloaded_launched_container.exit_code
                _retry(
                    lambda: reloaded_launched_container.upload_log(
                        launcher=self._launcher
                    )
                )
                # Skip downstream executions
                for execution_node in execution_nodes:
                    execution_node.container_execution_status = (
                        bts.ContainerExecutionStatus.FAILED
                    )
                    _mark_all_downstream_executions_as_skipped(
                        session=session, execution=execution_node
                    )
            else:
                _logger.error(
                    f"Container execution {container_execution.id} is now in unexpected state {new_status}. System error. {container_execution=}"
                )
                # This SYSTEM_ERROR will be handled by the outer exception handler
                raise OrchestratorError(
                    f"Unexpected running container status: {new_status=}, {launched_container=}"
                )
            session.commit()
        except:
            session.rollback()
            _logger.exception(f"Error processing {container_execution.id=}")
            container_execution.status = bts.ContainerExecutionStatus.SYSTEM_ERROR
            # Doing an intermediate commit here because it's most important to mark the problematic execution as SYSTEM_ERROR.
            session.commit()
            # Mark our ExecutionNode as SYSTEM_ERROR
            for execution_node in execution_nodes:
                execution_node.container_execution_status = (
                    bts.ContainerExecutionStatus.SYSTEM_ERROR
                )
            # Doing an intermediate commit here because it's most important to mark the problematic node as SYSTEM_ERROR.
            session.commit()
            # Skip downstream executions
            for execution_node in execution_nodes:
                _mark_all_downstream_executions_as_skipped(
                    session=session, execution=execution_node
                )
            session.commit()


def _get_direct_downstream_executions(
    session: orm.Session, execution: bts.ExecutionNode
):
    return session.scalars(
        sql.select(bts.ExecutionNode)
        .select_from(bts.OutputArtifactLink)
        .where(bts.OutputArtifactLink.execution_id == execution.id)
        .join(bts.ArtifactNode)
        .join(bts.InputArtifactLink)
        .join(bts.ExecutionNode)
    ).all()


# TODO: Cover with tests
def _mark_all_downstream_executions_as_skipped(
    session: orm.Session,
    execution: bts.ExecutionNode,
    seen_execution_ids: set[bts.IdType] | None = None,
):
    if seen_execution_ids is None:
        seen_execution_ids = set()
    if execution.id in seen_execution_ids:
        return
    seen_execution_ids.add(execution.id)
    if execution.container_execution_status in {
        bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
    }:
        execution.container_execution_status = bts.ContainerExecutionStatus.SKIPPED

    # for artifact_node in execution.output_artifact_nodes:
    #     for downstream_execution in artifact_node.downstream_executions:
    for downstream_execution in _get_direct_downstream_executions(session, execution):
        _mark_all_downstream_executions_as_skipped(
            session=session,
            execution=downstream_execution,
            seen_execution_ids=seen_execution_ids,
        )


def _assert_type(value: typing.Any, typ: typing.Type[_T]) -> _T:
    if not isinstance(value, typ):
        raise TypeError(f"Expected type {typ}, but got {type(value)}: {value}")
    return value


def _assert_not_none(value: _T | None) -> _T:
    if value is None:
        raise TypeError(f"Expected value to be not None, but got {value}.")
    return value


def _calculate_hash(s: str) -> str:
    import hashlib

    return "md5=" + hashlib.md5(s.encode("utf-8")).hexdigest()


def _calculate_container_execution_cache_key(
    container_spec: structures.ContainerSpec,
    input_artifact_data: dict[str, bts.ArtifactData],
):
    # Using ContainerSPec instead of the whole ComponentSpec.
    input_hashes = {
        input_name: _assert_not_none(artifact_data).hash
        for input_name, artifact_data in (input_artifact_data or {}).items()
    }
    cache_key_struct = {
        "container_spec": container_spec.to_json_dict(),
        "input_hashes": input_hashes,
    }
    # Need to sort keys to ensure consistency
    cache_key_str = json.dumps(cache_key_struct, separators=(",", ":"), sort_keys=True)
    # We could use a different hash function, but there is no reason to.
    cache_key = _calculate_hash(cache_key_str)
    return cache_key


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def _generate_random_id() -> str:
    import os
    import time

    random_bytes = os.urandom(4)
    nanoseconds = time.time_ns()
    milliseconds = nanoseconds // 1_000_000

    return ("%012x" % milliseconds) + random_bytes.hex()


def _update_dict_recursive(d1: dict, d2: dict):
    for k, v2 in d2.items():
        if k in d1:
            v1 = d1[k]
            if isinstance(v1, dict) and isinstance(v2, dict):
                _update_dict_recursive(v1, v2)
                continue
            # elif isinstance(v1, list) and isinstance(v2, list):
            # # Merging lists is not supported yet
        d1[k] = v2


def _retry(
    func: typing.Callable[[], _T], max_retries: int = 5, wait_seconds: float = 1.0
) -> _T:
    for i in range(max_retries):
        try:
            return func()
        except:
            _logger.exception(f"Exception calling {func}.")
            time.sleep(wait_seconds)
            if i == max_retries - 1:
                raise
    raise
