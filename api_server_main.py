import os
import traceback

import fastapi

from cloud_pipelines_backend import api_router

app = fastapi.FastAPI(
    title="Cloud Pipelines API",
    version="0.0.1",
    separate_input_output_schemas=False,
)


@app.exception_handler(Exception)
def handle_error(request: fastapi.Request, exc: BaseException):
    exception_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return fastapi.responses.JSONResponse(
        status_code=503,
        content={"exception": exception_str},
    )


DEFAULT_DATABASE_URI = "sqlite:///db.sqlite"
database_uri = (
    os.environ.get("DATABASE_URI")
    or os.environ.get("DATABASE_URL")
    or DEFAULT_DATABASE_URI
)

db_engine = api_router.create_db_engine(
    database_uri=database_uri,
)

api_router.setup_routes(app=app, db_engine=db_engine)
