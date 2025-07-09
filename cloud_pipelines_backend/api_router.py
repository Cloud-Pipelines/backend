import contextlib
import typing

import fastapi
import sqlalchemy
from sqlalchemy import orm

from . import api_server_sql
from . import backend_types_sql

if typing.TYPE_CHECKING:
    from .launchers import interfaces as launcher_interfaces


def setup_routes(
    app: fastapi.FastAPI,
    database_uri: str,
    get_user_name: typing.Callable[[], str] | None = None,
    ensure_admin_user: typing.Callable | None = None,
    container_launcher_for_log_streaming: (
        "launcher_interfaces.ContainerTaskLauncher | None"
    ) = None,
    db_connection_pool_size: int | None = None,
    db_connection_pool_max_overflow: int | None = None,
):
    # We request `app: fastapi.FastAPI` instead of just returning the router
    # because we want to add exception handler which is only supported for `FastAPI`.

    @app.exception_handler(api_server_sql.ItemNotFoundError)
    def handle_not_found_error(
        request: fastapi.Request, exc: api_server_sql.ItemNotFoundError
    ):
        return fastapi.responses.JSONResponse(
            status_code=404,
            content={"message": str(exc)},
        )

    if database_uri.startswith("mysql://"):
        try:
            import MySQLdb
        except ImportError:
            # Using PyMySQL instead of missing MySQLdb
            database_uri = database_uri.replace("mysql://", "mysql+pymysql://")

    db_engine = sqlalchemy.create_engine(
        url=database_uri,
        # echo=True,
        # Preventing the "MySQL server has gone away" error:
        # https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_size=db_connection_pool_size,
        max_overflow=db_connection_pool_max_overflow,
        # FastApi claims it's needed and safe: https://fastapi.tiangolo.com/tutorial/sql-databases/#create-an-engine
        connect_args=(
            {"check_same_thread": False} if database_uri.startswith("sqlite://") else {}
        ),
        # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#using-a-memory-database-in-multiple-threads
        poolclass=(sqlalchemy.pool.StaticPool if database_uri == "sqlite://" else None),
    )

    def get_session():
        with orm.Session(autocommit=False, autoflush=False, bind=db_engine) as session:
            yield session

    def create_db_and_tables():
        backend_types_sql._TableBase.metadata.create_all(db_engine)

    artifact_service = api_server_sql.ArtifactNodesApiService_Sql()
    execution_service = api_server_sql.ExecutionNodesApiService_Sql()
    pipeline_run_service = api_server_sql.PipelineRunsApiService_Sql()

    # Can be used by admins to disable methods that modify the DB
    is_read_only = False

    def check_not_readonly():
        if is_read_only:
            raise fastapi.HTTPException(
                status_code=503, detail="The server is in read-only mode."
            )

    # === API ===

    @contextlib.asynccontextmanager
    async def lifespan(app: fastapi.FastAPI):
        create_db_and_tables()
        yield

    router = fastapi.APIRouter(
        lifespan=lifespan,
    )

    default_config: dict = dict(
        response_model_exclude_defaults=True,
        response_model_exclude_none=True,
    )

    SessionDep = typing.Annotated[orm.Session, fastapi.Depends(get_session)]

    router.get("/api/artifacts/{id}", tags=["artifacts"], **default_config)(
        # functools.partial(print_annotations(artifact_service.get), session=fastapi.Depends(get_session))
        replace_annotations(artifact_service.get, orm.Session, SessionDep)
    )
    router.get("/api/executions/{id}/details", tags=["executions"], **default_config)(
        replace_annotations(execution_service.get, orm.Session, SessionDep)
    )
    get_graph_execution_state = replace_annotations(
        execution_service.get_graph_execution_state, orm.Session, SessionDep
    )
    # Deprecated
    router.get("/api/executions/{id}/state", tags=["executions"], **default_config)(
        get_graph_execution_state
    )
    router.get(
        "/api/executions/{id}/graph_execution_state",
        tags=["executions"],
        **default_config,
    )(get_graph_execution_state)
    router.get(
        "/api/executions/{id}/container_state", tags=["executions"], **default_config
    )(
        replace_annotations(
            execution_service.get_container_execution_state, orm.Session, SessionDep
        )
    )
    router.get("/api/executions/{id}/artifacts", tags=["executions"], **default_config)(
        replace_annotations(execution_service.get_artifacts, orm.Session, SessionDep)
    )

    @router.get(
        "/api/executions/{id}/container_log", tags=["executions"], **default_config
    )
    def get_container_log(
        id: backend_types_sql.IdType,
        session: SessionDep,
    ) -> api_server_sql.GetContainerExecutionLogResponse:
        return execution_service.get_container_execution_log(
            id=id,
            session=session,
            container_launcher=container_launcher_for_log_streaming,
        )

    if container_launcher_for_log_streaming:

        @router.get(
            "/api/executions/{id}/stream_container_log",
            tags=["executions"],
            **default_config,
        )
        def stream_container_log(
            id: backend_types_sql.IdType,
            session: SessionDep,
        ):
            iterator = execution_service.stream_container_execution_log(
                session=session,
                container_launcher=container_launcher_for_log_streaming,
                execution_id=id,
            )
            return fastapi.responses.StreamingResponse(
                iterator,
                headers={"X-Content-Type-Options": "nosniff"},
                media_type="text/event-stream",
            )

    list_pipeline_runs_func = pipeline_run_service.list
    if get_user_name:
        # The `created_by` parameter value now comes from a Dependency (instead of request)
        list_pipeline_runs_func = add_parameter_annotation_metadata(
            list_pipeline_runs_func,
            parameter_name="current_user",
            annotation_metadata=fastapi.Depends(get_user_name),
        )

    router.get("/api/pipeline_runs/", tags=["pipelineRuns"], **default_config)(
        replace_annotations(list_pipeline_runs_func, orm.Session, SessionDep)
    )
    router.get("/api/pipeline_runs/{id}", tags=["pipelineRuns"], **default_config)(
        replace_annotations(pipeline_run_service.get, orm.Session, SessionDep)
    )

    create_run_func = pipeline_run_service.create
    # The `session` parameter value now comes from a Dependency (instead of request)
    create_run_func = replace_annotations(create_run_func, orm.Session, SessionDep)
    if get_user_name:
        # The `created_by` parameter value now comes from a Dependency (instead of request)
        create_run_func = add_parameter_annotation_metadata(
            create_run_func,
            parameter_name="created_by",
            annotation_metadata=fastapi.Depends(get_user_name),
        )

    router.post(
        "/api/pipeline_runs/",
        tags=["pipelineRuns"],
        dependencies=[fastapi.Depends(check_not_readonly)],
        **default_config,
    )(create_run_func)

    router.get(
        "/api/artifacts/{id}/signed_artifact_url", tags=["artifacts"], **default_config
    )(
        replace_annotations(
            artifact_service.get_signed_artifact_url, orm.Session, SessionDep
        )
    )

    def pipeline_run_cancel(
        session: SessionDep, id: backend_types_sql.IdType, terminated_by: str
    ):
        skip_user_check = False
        try:
            if ensure_admin_user:
                ensure_admin_user()
            skip_user_check = True
        except:
            pass
        pipeline_run_service.terminate(
            session=session,
            id=id,
            terminated_by=terminated_by,
            skip_user_check=skip_user_check,
        )

    if get_user_name:
        # The `created_by` parameter value now comes from a Dependency (instead of request)
        pipeline_run_cancel = add_parameter_annotation_metadata(
            pipeline_run_cancel,
            parameter_name="terminated_by",
            annotation_metadata=fastapi.Depends(get_user_name),
        )
    router.post(
        "/api/pipeline_runs/{id}/cancel",
        tags=["pipelineRuns"],
        dependencies=[fastapi.Depends(check_not_readonly)],
        **default_config,
    )(pipeline_run_cancel)

    @router.put(
        "/api/admin/set_read_only_model",
        tags=["admin"],
        # Hiding the admin methods from the public schema.
        # include_in_schema=False,
        dependencies=(
            [fastapi.Depends(ensure_admin_user)] if ensure_admin_user else None
        ),
        **default_config,
    )
    def admin_set_read_only_model(read_only: bool):
        nonlocal is_read_only
        is_read_only = read_only

    @router.put(
        "/api/admin/execution_node/{id}/status",
        tags=["admin"],
        # Hiding the admin methods from the public schema.
        # include_in_schema=False,
        dependencies=(
            [fastapi.Depends(ensure_admin_user)] if ensure_admin_user else None
        ),
        **default_config,
    )
    def admin_set_execution_node_status(
        id: backend_types_sql.IdType,
        status: backend_types_sql.ContainerExecutionStatus,
        session: typing.Annotated[orm.Session, fastapi.Depends(get_session)],
    ):
        with session.begin():
            execution_node = session.get(backend_types_sql.ExecutionNode, id)
            execution_node.container_execution_status = status

    @router.get("/api/admin/sql_engine_connection_pool_status")
    async def get_sql_engine_connection_pool_status(
        session: typing.Annotated[orm.Session, fastapi.Depends(get_session)],
    ) -> str:
        return session.get_bind().pool.status()

    # # Needs to be called after all routes have been added to the router
    # app.include_router(router)

    app.include_router(router=router)


# def partial_wraps(func, **kwargs):
#     import inspect
#
#     # return functools.wraps(func)(functools.partial(func, *args, **kwargs))
#     partial_func = functools.partial(func, **kwargs)
#     param_names = set(kwargs)
#     signature = inspect.signature(func)
#     partial_signature = signature.replace(
#         parameters=[
#             parameter
#             for parameter_name, parameter in signature.parameters.items()
#             if parameter_name not in param_names
#         ]
#     )
#     print(f"{partial_signature=}")
#     partial_func.__signature__ = partial_signature
#     return partial_func


# Super hack to avoid duplicating functions in Fast API
# Normal functions take `session: orm.Session`.
# Those functions don't depend on FastApi, so we don't want to change those signatures.
# But FastApi needs `session: orm.Session = fastapi.Depends(get_session)`.
# functools.partial can add `session=fastapi.Depends(get_session)`, but destroys the signature.
# My partial_wraps attempt did not succeed.
# So the solution is to use `Annotated` method of dependency injection.
# https://fastapi.tiangolo.com/tutorial/dependencies
# We'll replace the original function annotations.
# It works!!!
def replace_annotations(func, original_annotation, new_annotation):
    for name in list(func.__annotations__):
        if func.__annotations__[name] == original_annotation:
            func.__annotations__[name] = new_annotation
    return func


def add_parameter_annotation_metadata(func, parameter_name: str, annotation_metadata):
    func.__annotations__[parameter_name] = typing.Annotated[
        func.__annotations__[parameter_name], annotation_metadata
    ]
    return func
