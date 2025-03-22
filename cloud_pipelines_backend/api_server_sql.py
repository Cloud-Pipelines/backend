import dataclasses
import datetime
import logging
import typing
from typing import Any, Optional


_logger = logging.getLogger(__name__)

T = typing.TypeVar("T")


class ApiServiceError(RuntimeError):
    pass


def _get_current_time() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


from . import component_structures as structures
from . import backend_types_sql as bts


class ItemNotFoundError(Exception):
    pass


# ==== PipelineJobService
@dataclasses.dataclass(kw_only=True)
class PipelineRunResponse:
    id: int
    root_execution_id: bts.IdType
    annotations: dict[str, Any] | None = None
    # status: "PipelineJobStatus"
    created_by: str | None = None
    created_at: datetime.datetime | None = None

    @classmethod
    def from_db(cls, pipeline_run: bts.PipelineRun) -> "PipelineRunResponse":
        return PipelineRunResponse(
            id=pipeline_run.id,
            root_execution_id=pipeline_run.root_execution_id,
            annotations=pipeline_run.annotations,
            created_by=pipeline_run.created_by,
            created_at=pipeline_run.created_at,
        )


class GetPipelineRunResponse(PipelineRunResponse):
    pass


@dataclasses.dataclass(kw_only=True)
class ListPipelineJobsResponse:
    pipeline_runs: list[PipelineRunResponse]
    next_page_token: str | None = None


import sqlalchemy as sql
from sqlalchemy import orm


class PipelineRunsApiService_Sql:

    def create(
        self,
        session: orm.Session,
        root_task: structures.TaskSpec,
        # Component library to avoid repeating component specs inside task specs
        components: Optional[list[structures.ComponentReference]] = None,
        # Arbitrary metadata. Can be used to specify user.
        annotations: Optional[dict[str, Any]] = None,
        created_by: str | None = None,
    ) -> PipelineRunResponse:
        # TODO: Validate the pipeline spec
        # TODO: Load and validate all components
        # TODO: Fetch missing components and populate component specs

        with session.begin():

            root_execution_node = _recursively_create_all_executions_and_artifacts_root(
                session=session,
                root_task_spec=root_task,
            )

            # Store into DB.
            current_time = _get_current_time()
            pipeline_run = bts.PipelineRun(
                root_execution=root_execution_node,
                created_at=current_time,
                updated_at=current_time,
                annotations=annotations,
                created_by=created_by,
            )
            session.add(pipeline_run)
            session.commit()

        session.refresh(pipeline_run)
        return PipelineRunResponse.from_db(pipeline_run)

    def get(self, session: orm.Session, id: bts.IdType) -> PipelineRunResponse:
        pipeline_run = session.get(bts.PipelineRun, id)
        if not pipeline_run:
            raise ItemNotFoundError(f"Pipeline run {id} not found.")
        return PipelineRunResponse.from_db(pipeline_run)

    # Note: This method must be last to not shadow the "list" type
    def list(
        self,
        session: orm.Session,
        page_token: str | None = None,
        # page_size: int  = 10,
        # filter: str | None = None,
    ) -> ListPipelineJobsResponse:
        # if filter:
        #     raise NotImplementedError("filter is not implemented yet")

        # TODO: Implement proper page tokens
        page_idx = int(page_token) if page_token else 0
        page_size = 10
        offset = page_idx * page_size

        pipeline_runs = list(
            session.scalars(
                sql.select(bts.PipelineRun)
                .order_by(bts.PipelineRun.created_at.desc())
                .offset(offset)
                .limit(page_size)
            ).all()
        )
        next_page_token = str(page_idx + page_size)
        return ListPipelineJobsResponse(
            pipeline_runs=[
                PipelineRunResponse.from_db(pipeline_run)
                for pipeline_run in pipeline_runs
            ],
            next_page_token=next_page_token,
        )


# ========== ExecutionNodeApiService_Mongo


def _calculate_hash(s: str) -> str:
    import hashlib

    return "md5=" + hashlib.md5(s.encode("utf-8")).hexdigest()


def _split_type_spec(
    type_spec: structures.TypeSpecType | None,
) -> typing.Tuple[str | None, dict[str, Any] | None]:
    if type_spec is None:
        return None, None
    if isinstance(type_spec, str):
        return type_spec, None
    if isinstance(type_spec, typing.Mapping):
        kv_pairs = list(type_spec.items())
        if len(kv_pairs) == 1:
            type_name, type_properties = kv_pairs[1]
            if isinstance(type_name, str) and isinstance(
                type_properties, typing.Mapping
            ):
                return type_name, dict(type_properties)
    raise TypeError(f"Unsupported kind of type spec: {type_spec}")


# def _construct_constant_data_info(value: str) -> DataInfo:
#     return DataInfo(
#         total_size=len(value),
#         is_dir=False,
#         hash=_calculate_hash(value),
#     )


def _construct_constant_artifact_data(value: str) -> bts.ArtifactData:
    # FIX: !!!
    # raise NotImplementedError("MUST insert into session. Need to de-duplicate")
    artifact_data = bts.ArtifactData(
        total_size=len(value),
        is_dir=False,
        hash=_calculate_hash(value),
        value=value,
        created_at=_get_current_time(),
    )
    return artifact_data


def _construct_constant_artifact_node(
    value: str,
    artifact_type: structures.TypeSpecType | None = None,
):
    type_name, type_properties = _split_type_spec(artifact_type)
    artifact_node = bts.ArtifactNode(
        type_name=type_name,
        type_properties=type_properties,
        artifact_data=_construct_constant_artifact_data(value=value),
        had_data_in_past=True,
    )
    return artifact_node


def _construct_constant_artifact_node_and_add_to_session(
    session: orm.Session,
    value: str,
    artifact_type: structures.TypeSpecType | None = None,
):
    # FIX: !!!
    # raise NotImplementedError("MUST insert into session. Need to de-duplicate")
    artifact_node = _construct_constant_artifact_node(
        value=value, artifact_type=artifact_type
    )
    session.add(artifact_node.artifact_data)
    session.add(artifact_node)
    return artifact_node


# ? Do we need an association table between PipelineJob and ExecutionNode


# ============


@dataclasses.dataclass(kw_only=True)
class GetExecutionInfoResponse:
    id: bts.IdType
    task_spec: structures.TaskSpec
    parent_execution_id: bts.IdType | None = None
    child_task_execution_ids: dict[str, bts.IdType]
    # pipeline_run_id: str
    # ancestor_breadcrumbs: list[tuple[str, str]]
    input_artifacts: dict[str, "ArtifactNodeIdResponse"] | None = None
    output_artifacts: dict[str, "ArtifactNodeIdResponse"] | None = None


@dataclasses.dataclass
class ArtifactNodeIdResponse:
    id: bts.IdType


@dataclasses.dataclass
class GetGraphExecutionStateResponse:
    child_execution_status_stats: dict[bts.IdType, dict[str, int]]
    pass


@dataclasses.dataclass(kw_only=True)
class GetExecutionArtifactsResponse:
    input_artifacts: dict[str, "ArtifactNodeResponse"] | None = None
    output_artifacts: dict[str, "ArtifactNodeResponse"] | None = None


class ExecutionNodesApiService_Sql:

    def get(self, session: orm.Session, id: bts.IdType) -> GetExecutionInfoResponse:
        execution_node = session.get(bts.ExecutionNode, id)
        if execution_node is None:
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")

        child_executions = execution_node.child_executions
        child_task_execution_ids = {
            child_execution.task_id_in_parent_execution
            or "<missing>": child_execution.id
            for child_execution in child_executions
        }
        input_artifacts = {
            input_name: ArtifactNodeIdResponse(id=artifact_id)
            for input_name, artifact_id in session.execute(
                sql.select(
                    bts.InputArtifactLink.input_name, bts.InputArtifactLink.artifact_id
                ).where(bts.InputArtifactLink.execution_id == id)
            ).tuples()
        }
        output_artifacts = {
            output_name: ArtifactNodeIdResponse(id=artifact_id)
            for output_name, artifact_id in session.execute(
                sql.select(
                    bts.OutputArtifactLink.output_name,
                    bts.OutputArtifactLink.artifact_id,
                ).where(bts.InputArtifactLink.execution_id == id)
            ).tuples()
        }
        return GetExecutionInfoResponse(
            id=execution_node.id,
            task_spec=structures.TaskSpec.from_json_dict(execution_node.task_spec),
            parent_execution_id=execution_node.parent_execution_id,
            child_task_execution_ids=child_task_execution_ids,
            input_artifacts=input_artifacts,
            output_artifacts=output_artifacts,
        )

    def get_graph_execution_state(
        self, session: orm.Session, id: bts.IdType
    ) -> GetGraphExecutionStateResponse:
        ExecutionNode_Child = orm.aliased(
            bts.ExecutionNode, name="child_execution_node"
        )
        ExecutionNode_Descendant = orm.aliased(
            bts.ExecutionNode, name="descendant_execution_node"
        )
        # # We cannot use this query since ContainerExecution do not exist
        # # for not yet started container execution nodes.
        # query = (
        #     sql.select(
        #         ExecutionNode_Child.id.label("child_execution_id"),
        #         bts.ContainerExecution.status,
        #         sql.func.count().label("count"),
        #     )
        #     .where(ExecutionNode_Child.parent_execution_id == id)
        #     .join(
        #         bts.ExecutionToAncestorExecutionLink,
        #         bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
        #         == ExecutionNode_Child.id,
        #     )
        #     .join(
        #         ExecutionNode_Descendant,
        #         ExecutionNode_Descendant.id
        #         == bts.ExecutionToAncestorExecutionLink.execution_id,
        #     )
        #     .join(
        #         bts.ContainerExecution,
        #         bts.ContainerExecution.id
        #         == ExecutionNode_Descendant.container_execution_id,
        #     )
        #     .group_by(
        #         ExecutionNode_Child.id,
        #         bts.ContainerExecution.status,
        #     )
        # )
        child_descendants_query = (
            sql.select(
                ExecutionNode_Child.id.label("child_execution_id"),
                ExecutionNode_Descendant.container_execution_status,
                sql.func.count().label("count"),
            )
            .where(ExecutionNode_Child.parent_execution_id == id)
            .join(
                bts.ExecutionToAncestorExecutionLink,
                bts.ExecutionToAncestorExecutionLink.ancestor_execution_id
                == ExecutionNode_Child.id,
            )
            .join(
                ExecutionNode_Descendant,
                ExecutionNode_Descendant.id
                == bts.ExecutionToAncestorExecutionLink.execution_id,
            )
            .where(ExecutionNode_Descendant.container_execution_status != None)
            .group_by(
                ExecutionNode_Child.id,
                ExecutionNode_Descendant.container_execution_status,
            )
        )
        direct_container_children_query = (
            sql.select(
                ExecutionNode_Child.id.label("child_execution_id"),
                ExecutionNode_Child.container_execution_status,
                sql.func.count().label("count"),
            )
            .where(ExecutionNode_Child.parent_execution_id == id)
            .where(ExecutionNode_Child.container_execution_status != None)
            .group_by(
                ExecutionNode_Child.id,
                ExecutionNode_Child.container_execution_status,
            )
        )
        child_descendants_execution_stat_rows = session.execute(
            child_descendants_query
        ).all()
        child_container_execution_stat_rows = session.execute(
            direct_container_children_query
        ).all()
        child_execution_stat_rows = tuple(
            child_descendants_execution_stat_rows
        ) + tuple(child_container_execution_stat_rows)
        child_execution_status_stats: dict[bts.IdType, dict[str, int]] = {}

        for row in child_execution_stat_rows:
            child_execution_id, status, count = row.tuple()
            status_stats = child_execution_status_stats.setdefault(
                child_execution_id, {}
            )
            status_stats[status.value] = count
        return GetGraphExecutionStateResponse(
            child_execution_status_stats=child_execution_status_stats,
        )

    def get_artifacts(
        self, session: orm.Session, id: bts.IdType
    ) -> GetExecutionArtifactsResponse:
        if not session.scalar(
            sql.select(sql.exists().where(bts.ExecutionNode.id == id))
        ):
            raise ItemNotFoundError(f"Execution with {id=} does not exist.")

        input_artifact_links = session.scalars(
            sql.select(bts.InputArtifactLink)
            .where(bts.InputArtifactLink.execution_id == id)
            .options(
                orm.joinedload(bts.InputArtifactLink.artifact).joinedload(
                    bts.ArtifactNode.artifact_data
                )
            )
        )
        output_artifact_links = session.scalars(
            sql.select(bts.OutputArtifactLink)
            .where(bts.OutputArtifactLink.execution_id == id)
            .options(
                orm.joinedload(bts.OutputArtifactLink.artifact).joinedload(
                    bts.ArtifactNode.artifact_data
                )
            )
        )

        input_artifacts = {
            input_artifact_link.input_name: ArtifactNodeResponse.from_db(
                input_artifact_link.artifact
            )
            for input_artifact_link in input_artifact_links
        }
        output_artifacts = {
            output_artifact_link.output_name: ArtifactNodeResponse.from_db(
                output_artifact_link.artifact
            )
            for output_artifact_link in output_artifact_links
        }
        return GetExecutionArtifactsResponse(
            input_artifacts=input_artifacts,
            output_artifacts=output_artifacts,
        )


@dataclasses.dataclass(kw_only=True)
class ArtifactNodeResponse:
    id: bts.IdType
    # had_data_in_past: bool = False
    # may_have_data_in_future: bool = True
    type_name: str | None = None
    type_properties: dict[str, Any] | None = None
    producer_execution_id: bts.IdType | None = None
    producer_output_name: str | None = None
    # artifact_data_id: bts.IdType | None = None
    artifact_data: "ArtifactDataResponse | None" = None

    @classmethod
    def from_db(cls, artifact_node: bts.ArtifactNode) -> "ArtifactNodeResponse":
        result = ArtifactNodeResponse(
            **{
                field.name: getattr(artifact_node, field.name)
                for field in dataclasses.fields(ArtifactNodeResponse)
            }
        )
        if artifact_node.artifact_data:
            result.artifact_data = ArtifactDataResponse.from_db(
                artifact_data=artifact_node.artifact_data
            )
        return result


@dataclasses.dataclass(kw_only=True)
class ArtifactDataResponse:
    total_size: int
    is_dir: bool
    # hash: str
    # At least one of `uri` or `value` must be set
    uri: str | None = None
    # Small constant value
    value: str | None = None
    # created_at: datetime.datetime | None = None
    # deleted_at: datetime.datetime | None = None

    @classmethod
    def from_db(cls, artifact_data: bts.ArtifactData) -> "ArtifactDataResponse":
        return ArtifactDataResponse(
            **{
                field.name: getattr(artifact_data, field.name)
                for field in dataclasses.fields(ArtifactDataResponse)
            }
        )


@dataclasses.dataclass(kw_only=True)
class GetArtifactInfoResponse:
    id: bts.IdType
    artifact_data: bts.ArtifactData | None = None


@dataclasses.dataclass(kw_only=True)
class GetArtifactSignedUrlResponse:
    signed_url: str


class ArtifactNodesApiService_Sql:

    def get(self, session: orm.Session, id: bts.IdType) -> GetArtifactInfoResponse:
        artifact_node = session.get(bts.ArtifactNode, id)
        if artifact_node is None:
            raise ItemNotFoundError(f"Artifact with {id=} does not exist.")
        artifact_data = artifact_node.artifact_data
        result = GetArtifactInfoResponse(id=artifact_node.id)
        if artifact_data:
            result.artifact_data = artifact_data
        return result

    def get_signed_artifact_url(
        self, session: orm.Session, id: bts.IdType
    ) -> GetArtifactSignedUrlResponse:
        artifact_data = session.scalar(
            sql.select(bts.ArtifactData)
            .join(bts.ArtifactNode)
            .where(bts.ArtifactNode.id == id)
        )
        if not artifact_data:
            raise ItemNotFoundError(f"Artifact node with {id=} does not exist.")
        if not artifact_data.uri:
            raise ValueError(f"Artifact node with {id=} does not have artifact URI.")
        if artifact_data.is_dir:
            raise ValueError(f"Cannot generate signer URL for a directory artifact.")
        if not artifact_data.uri.startswith("gs://"):
            raise ValueError(
                f"The get_signed_artifact_url method only supports Google Cloud Storage URIs, but got {artifact_data.uri=}."
            )

        from google.cloud import storage

        storage_client = storage.Client()
        blob = storage.Blob.from_string(uri=artifact_data.uri, client=storage_client)
        signed_url = blob.generate_signed_url(
            # Expiration is required. Max expiration value is 7 days.
            expiration=datetime.timedelta(days=7)
        )
        return GetArtifactSignedUrlResponse(signed_url=signed_url)


# ============

# Idea for how to add deep nested graph:
# First: Recursively create all task execution nodes and create their output artifacts
# Then: For each execution node starting from root:
#       Set/create input argument artifacts
#       If the node is a graph, process the node's children
# ---
# No. Decided to first do topological sort and then 1-stage generation.


def _recursively_create_all_executions_and_artifacts_root(
    session: orm.Session,
    root_task_spec: structures.TaskSpec,
) -> bts.ExecutionNode:
    input_artifact_nodes: dict[str, bts.ArtifactNode] = {}

    root_component_spec = root_task_spec.component_ref.spec
    if not root_component_spec:
        raise ApiServiceError(
            f"root_task_spec.component_ref.spec is empty. {root_task_spec=}"
        )
    input_specs = {
        input_spec.name: input_spec for input_spec in root_component_spec.inputs or []
    }
    for input_name, input_argument in (root_task_spec.arguments or {}).items():
        input_spec = input_specs.get(input_name)
        if not input_spec:
            raise ApiServiceError(
                f"Argument given for non-existing input '{input_name}'. {root_task_spec=}"
            )
        if isinstance(
            input_argument,
            (
                structures.GraphInputArgument,
                structures.TaskOutputArgument,
            ),
        ):
            raise ApiServiceError(
                f"root task arguments can only be constants, but got {input_name}={input_argument}. {root_task_spec=}"
            )
        elif not isinstance(input_argument, str):
            raise ApiServiceError(
                f"root task constant argument must be a string, but got {input_name}={input_argument}. {root_task_spec=}"
            )
        # TODO: Support constant input artifacts (artifact IDs)
        if input_argument is not None:
            input_artifact_nodes[input_name] = (
                # _construct_constant_artifact_node_and_add_to_session(
                #     session=session, value=input_argument, artifact_type=input_spec.type
                # )
                _construct_constant_artifact_node(
                    value=input_argument, artifact_type=input_spec.type
                )
            )
            # This constant artifact won't be added to the DB
            # TODO: Actually, they will be added...
            # We don't need to link this input artifact here. It will be handled downstream.

    root_execution_node = _recursively_create_all_executions_and_artifacts(
        session=session,
        root_task_spec=root_task_spec,
        input_artifact_nodes=input_artifact_nodes,
        ancestors=[],
    )
    return root_execution_node


def _recursively_create_all_executions_and_artifacts(
    session: orm.Session,
    root_task_spec: structures.TaskSpec,
    input_artifact_nodes: dict[str, bts.ArtifactNode],
    ancestors: list[bts.ExecutionNode],
) -> bts.ExecutionNode:
    root_component_spec = root_task_spec.component_ref.spec
    if not root_component_spec:
        raise ApiServiceError(
            f"root_task.component_ref.spec is empty. {root_task_spec=}"
        )

    implementation = root_component_spec.implementation
    if not implementation:
        raise ApiServiceError(
            f"component_spec.implementation is empty. {root_task_spec=}"
        )

    root_execution_node = bts.ExecutionNode(
        task_spec=root_task_spec.to_json_dict(),
        # child_task_id_to_execution_node=None,
    )
    session.add(root_execution_node)
    for ancestor in ancestors:
        ancestor_link = bts.ExecutionToAncestorExecutionLink(
            execution=root_execution_node,
            ancestor_execution=ancestor,
        )
        session.add(ancestor_link)

    # FIX: Handle ExecutionNode.constant_arguments
    # We do not touch root_task_spec.arguments. We use graph_input_artifact_nodes instead
    constant_input_artifacts: dict[str, bts.ArtifactData] = {}
    input_artifact_nodes = dict(input_artifact_nodes)
    for input_spec in root_component_spec.inputs or []:
        input_artifact_node = input_artifact_nodes.get(input_spec.name)
        if input_artifact_node is None and not input_spec.optional:
            if input_spec.default:
                input_artifact_node = (
                    _construct_constant_artifact_node_and_add_to_session(
                        session=session,
                        value=input_spec.default,
                        artifact_type=input_spec.type,
                    )
                )
                # # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
                # input_artifact_node = (
                #     _construct_constant_artifact_node(
                #         value=input_spec.default,
                #         artifact_type=input_spec.type,
                #     )
                # )
                # This constant artifact won't be added to the DB
                # result_artifact_nodes.append(artifact_node)
                input_artifact_nodes[input_spec.name] = input_artifact_node
            else:
                raise ApiServiceError(
                    f"Task has a required input {input_spec.name}, but no upstream artifact and no default value. {root_task_spec=}"
                )
        if input_artifact_node:
            # if input_artifact_node.artifact_data:
            #     # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
            #     constant_input_artifacts[input_spec.name] = input_artifact_node.artifact_data
            # else:
            input_artifact_link = bts.InputArtifactLink(
                execution=root_execution_node,
                input_name=input_spec.name,
                artifact=input_artifact_node,
            )
            session.add(input_artifact_link)

    if isinstance(implementation, structures.ContainerImplementation):
        for output_spec in root_component_spec.outputs or []:
            artifact_node = bts.ArtifactNode(
                producer_execution=root_execution_node,
                producer_output_name=output_spec.name,
                # TODO: Improve type handling
                type_name=_split_type_spec(output_spec.type)[0],
                type_properties=_split_type_spec(output_spec.type)[1],
                artifact_data=None,
            )
            session.add(artifact_node)

            output_artifact_link = bts.OutputArtifactLink(
                execution=root_execution_node,
                output_name=output_spec.name,
                artifact=artifact_node,
            )
            session.add(output_artifact_link)

        # FIX!: Create ContainerExecution here. (Beware of caching.)
        # # container_spec = implementation.container
        # container_execution_node = ContainerExecutionNode(
        #     id=...,
        #     status=ContainerExecutionStatus.WaitingForUpstream
        # )
        # root_execution_node.container_execution_id = container_execution_node.id
        # Done: Maybe set WAITING_FOR_UPSTREAM ourselves.
        root_execution_node.container_execution_status = (
            bts.ContainerExecutionStatus.QUEUED
            if all(
                artifact_node.artifact_data
                for artifact_node in input_artifact_nodes.values()
            )
            else bts.ContainerExecutionStatus.WAITING_FOR_UPSTREAM
        )
    elif isinstance(implementation, structures.GraphImplementation):
        # Processing child tasks
        graph_spec = implementation.graph

        # task_id_to_execution_node: dict[str, bts.ExecutionNode] = {}
        task_output_artifact_nodes: dict[str, dict[str, bts.ArtifactNode]] = {}

        # Implementation design:
        # We need to either toposort tasks or delay processing input arguments
        # until ALL execution nodes and output artifacts are constructed (recursively).
        # Let's try to use topological sort.
        # This will also allow us to test for cycles.

        child_tasks = _toposort_tasks(graph_spec.tasks)

        # Processing child task input artifacts
        for child_task_id, child_task_spec in child_tasks.items():
            child_component_spec = child_task_spec.component_ref.spec
            if not child_component_spec:
                raise ApiServiceError(
                    f"child_task_spec.component_ref.spec is empty. {child_task_spec=}"
                )
            child_task_input_artifact_nodes: dict[str, bts.ArtifactNode] = {}
            for input_spec in child_component_spec.inputs or []:
                input_argument = (child_task_spec.arguments or {}).get(input_spec.name)
                input_artifact_node: bts.ArtifactNode | None = None
                if input_argument is None and not input_spec.optional:
                    # Not failing on unconnected required input if there is a default value
                    if input_spec.default is None:
                        raise ApiServiceError(
                            f"Task has a required input '{input_spec.name}', but no upstream artifact and no default value. {child_task_spec=}"
                        )
                    else:
                        input_argument = input_spec.default
                if input_argument is None:
                    pass
                elif isinstance(input_argument, structures.GraphInputArgument):
                    input_artifact_node = input_artifact_nodes.get(
                        input_argument.graph_input.input_name
                    )
                    if input_artifact_node is None:
                        # Warning: unconnected upstream
                        # TODO: Support using upstream graph input's default value when needed for required input (non-trivial feature).
                        # Feature: "Unconnected upstream with optional default"
                        pass
                elif isinstance(input_argument, structures.TaskOutputArgument):
                    task_output_source = input_argument.task_output
                    input_artifact_node = task_output_artifact_nodes[
                        task_output_source.task_id
                    ][task_output_source.output_name]
                elif isinstance(input_argument, str):
                    input_artifact_node = (
                        _construct_constant_artifact_node_and_add_to_session(
                            session=session,
                            value=input_argument,
                            artifact_type=input_spec.type,
                        )
                    )
                    # Not adding constant inputs to the DB. We'll add them to `ExecutionNode.constant_arguments`
                    # input_artifact_node = (
                    #     _construct_constant_artifact_node(
                    #         value=input_argument,
                    #         artifact_type=input_spec.type,
                    #     )
                    # )
                else:
                    raise ApiServiceError(
                        f"Unexpected task argument: {input_spec.name}={input_argument}. {child_task_spec=}"
                    )
                if input_artifact_node:
                    child_task_input_artifact_nodes[input_spec.name] = (
                        input_artifact_node
                    )

            # Creating child task nodes and their output artifacts
            child_execution_node = _recursively_create_all_executions_and_artifacts(
                session=session,
                root_task_spec=child_task_spec,
                input_artifact_nodes=child_task_input_artifact_nodes,
                ancestors=ancestors + [root_execution_node],
            )
            child_execution_node.parent_execution = root_execution_node
            child_execution_node.task_id_in_parent_execution = child_task_id
            # task_id_to_execution_node[child_task_id] = child_execution_node
            # TODO: ! Ensure this relationship works properly (is populated and does not query DB),
            task_output_artifact_nodes[child_task_id] = {
                link.output_name: link.artifact
                for link in child_execution_node.output_artifact_links
            }

        # Processing root graph output artifacts
        for output_name, output_source in (graph_spec.output_values or {}).items():
            if not isinstance(output_source, structures.TaskOutputArgument):
                raise ApiServiceError(
                    f"graph_spec.output_values values can only be of type TaskOutputArgument, but got {output_source=}"
                )
            task_output_source = output_source.task_output
            source_artifact = task_output_artifact_nodes[task_output_source.task_id][
                task_output_source.output_name
            ]
            output_artifact_link = bts.OutputArtifactLink(
                execution=root_execution_node,
                output_name=output_name,
                artifact=source_artifact,
            )
            session.add(output_artifact_link)
    else:
        raise ApiServiceError(
            f"Unknown ComponentSpec.implementation. {root_component_spec=}"
        )
    return root_execution_node


def _toposort_tasks(
    tasks: typing.Mapping[str, structures.TaskSpec],
) -> dict[str, structures.TaskSpec]:
    # Checking task output references and preparing the dependency table
    task_dependencies: dict[str, dict] = {}
    for task_id, task in tasks.items():
        # Using dict instead of set to stabilize the ordering
        dependencies: dict[str, bool] = {}
        task_dependencies[task_id] = dependencies
        if task.arguments is not None:
            for argument in task.arguments.values():
                if isinstance(argument, structures.TaskOutputArgument):
                    dependencies[argument.task_output.task_id] = True
                    if argument.task_output.task_id not in tasks:
                        raise TypeError(
                            'Argument "{}" references non-existing task.'.format(
                                argument
                            )
                        )

    # Topologically sorting tasks to detect cycles
    task_dependents = {k: {} for k in task_dependencies.keys()}
    for task_id, dependencies in task_dependencies.items():
        for dependency in dependencies:
            task_dependents[dependency][task_id] = True
    task_number_of_remaining_dependencies = {
        k: len(v) for k, v in task_dependencies.items()
    }
    sorted_tasks = {}  # Python dictionaries preserve order now

    def process_task(task_id):
        if (
            task_number_of_remaining_dependencies[task_id] == 0
            and task_id not in sorted_tasks
        ):
            sorted_tasks[task_id] = tasks[task_id]
            for dependent_task in task_dependents[task_id]:
                task_number_of_remaining_dependencies[dependent_task] -= 1
                process_task(dependent_task)

    for task_id in task_dependencies.keys():
        process_task(task_id)
    if len(sorted_tasks) != len(task_dependencies):
        tasks_with_unsatisfied_dependencies = {
            k: v for k, v in task_number_of_remaining_dependencies.items() if v > 0
        }
        task_with_minimal_number_of_unsatisfied_dependencies = min(
            tasks_with_unsatisfied_dependencies.keys(),
            key=lambda task_id: tasks_with_unsatisfied_dependencies[task_id],
        )
        raise ValueError(
            'Task "{}" has cyclical dependency.'.format(
                task_with_minimal_number_of_unsatisfied_dependencies
            )
        )

    return sorted_tasks


# ================
# 2025-02-15

# Idea: There are high-level abstract execution nodes. Some of them link to ContainerExecution (or possibly multiple of them is thee are retries.)
# There are edges in the DB. Different kinds of edges: Artifact passing edges, execution-to-parent-execution edges etc.
# Idea: When [container?] execution finishes, the system processes/activates/notifies the edges. For example, sets downstream execution's input artifacts.
# We could have a dedicated queue for edges (the upstream only "activates" the edges for processing), but we can start with the execution completion actively processing everything synchronously.
# There is a risk of race condition. To somewhat mitigate this risk, the code that wants to check the state and add the edge should add the edge first, then try to process it.


# class ArtifactPromiseEnum(enum.Enum):
#     NOT_CREATED_YET = 1
#     CREATED = 2
#     WILL_NEVER_BE_CREATED = 3  # = Upstream failed/skipped/cancelled
#     UPSTREAM_NOT_CONNECTED = 4
#     # Flags: have_existed, may_exist_in_future


# # There need to be special artifact: UnconnectedArtifactWithDefaultValue
# # If input is optional, it's treated as unconnected.
# # If input is required, it's treated as the default value.
# class UnconnectedArtifactWithDefaultValue:
#     default_value: str
