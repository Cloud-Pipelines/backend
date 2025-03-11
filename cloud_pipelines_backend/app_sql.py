import traceback

import fastapi

from . import api_server_sql
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


@app.exception_handler(api_server_sql.ItemNotFoundError)
def handle_not_found_error(
    request: fastapi.Request, exc: api_server_sql.ItemNotFoundError
):
    return fastapi.responses.JSONResponse(
        status_code=404,
        content={"message": str(exc)},
    )


# Needs to be called after all routes have been added to the router
app.include_router(api_router.router)
