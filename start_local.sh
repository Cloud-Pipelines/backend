#!/bin/sh

set -e -o pipefail -o nounset

current_path=`pwd`
cd $(dirname $0)

CLOUD_PIPELINES_BACKEND_DATA_DIR="${current_path}/data" uv run fastapi run start_local.py "$@"
