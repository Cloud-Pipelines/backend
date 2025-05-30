import os
import traceback

import fastapi

from . import api_router

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


DEFAULT_DATABASE_URI = "sqlite://"
database_uri = (
    os.environ.get("DATABASE_URI")
    or os.environ.get("DATABASE_URL")
    or DEFAULT_DATABASE_URI
)

api_router.setup_routes(app=app, database_uri=database_uri)
