@echo off
setlocal EnableDelayedExpansion

REM Set the backend data dir to $(pwd)/data
set "CLOUD_PIPELINES_BACKEND_DATA_DIR=%CD%\data"

REM `cd $(dirname) $0`
cd /d "%~dp0"

call uv run --frozen fastapi run start_local.py %*

exit /b %ERRORLEVEL%
