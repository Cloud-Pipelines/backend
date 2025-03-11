import contextlib
import functools
import os
import traceback
import typing

import fastapi
import sqlalchemy
from sqlalchemy import orm

from . import api_server_sql
from . import backend_types_sql


DEFAULT_DATABASE_URI = "sqlite://"
database_uri = os.environ.get("DATABASE_URI", DEFAULT_DATABASE_URI)

db_engine = sqlalchemy.create_engine(
    url=database_uri,
    # echo=True,
    # FastApi claims it's needed and safe: https://fastapi.tiangolo.com/tutorial/sql-databases/#create-an-engine
    connect_args={"check_same_thread": False},
    # https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#using-a-memory-database-in-multiple-threads
    poolclass=(
        sqlalchemy.pool.StaticPool if database_uri == DEFAULT_DATABASE_URI else None
    ),
)

session_factory = orm.sessionmaker(autocommit=False, autoflush=False, bind=db_engine)


def get_session():
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


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


# app = fastapi.FastAPI(
#     title="Cloud Pipelines API",
#     version="0.0.1",
#     separate_input_output_schemas=False,
#     lifespan=lifespan,
# )

router = fastapi.APIRouter(
    lifespan=lifespan,
)


# @app.exception_handler(Exception)
# def handle_error(request: fastapi.Request, exc: BaseException):
#     exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
#     return fastapi.responses.JSONResponse(
#         status_code=503,
#         content={"exception": exception_str},
#     )


# @app.exception_handler(api_server_sql.ItemNotFoundError)
# def handle_not_found_error(
#     request: fastapi.Request, exc: api_server_sql.ItemNotFoundError
# ):
#     return fastapi.responses.JSONResponse(
#         status_code=404,
#         content={"message": str(exc)},
#     )


default_route_config: dict = dict(
    response_model_exclude_defaults=True,
    response_model_exclude_none=True,
)


@router.put(
    "/api/admin/set_read_only_model",
    tags=["admin"],
    # Hiding the admin methods from the public schema.
    include_in_schema=False,
    **default_route_config,
)
def admin_set_read_only_model(read_only: bool):
    global is_read_only
    is_read_only = read_only


# def partial_wraps(func, **kwargs):
#     import inspect

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
SessionDep = typing.Annotated[orm.Session, fastapi.Depends(get_session)]


def replace_annotations(func, original_annotation, new_annotation):
    for name in list(func.__annotations__):
        if func.__annotations__[name] == original_annotation:
            func.__annotations__[name] = new_annotation
    return func


router.get("/api/artifacts/{id}", tags=["artifacts"], **default_route_config)(
    # functools.partial(print_annotations(artifact_service.get), session=fastapi.Depends(get_session))
    replace_annotations(artifact_service.get, orm.Session, SessionDep)
)
router.get("/api/executions/{id}/details", tags=["executions"], **default_route_config)(
    replace_annotations(execution_service.get, orm.Session, SessionDep)
)
router.get("/api/executions/{id}/state", tags=["executions"], **default_route_config)(
    replace_annotations(execution_service.get_state, orm.Session, SessionDep)
)
router.get("/api/pipeline_runs/", tags=["pipelineRuns"], **default_route_config)(
    replace_annotations(pipeline_run_service.list, orm.Session, SessionDep)
)
router.get("/api/pipeline_runs/{id}", tags=["pipelineRuns"], **default_route_config)(
    replace_annotations(pipeline_run_service.get, orm.Session, SessionDep)
)


router.post(
    "/api/pipeline_runs/",
    tags=["pipelineRuns"],
    dependencies=[fastapi.Depends(check_not_readonly)],
    **default_route_config,
)(replace_annotations(pipeline_run_service.create, orm.Session, SessionDep))


# # Needs to be called after all routes have been added to the router
# app.include_router(router)
