#!/bin/bash

################################################################################
# Local scratch database for this project: a thin wrapper around platform-db's
# shared local-db.sh (one local cluster, one database per project, named after
# package.json). Runs from the project root, so it works from any directory.
# Usage: local-db.sh <init|start|stop|status|psql|env|run CMD…|serve|reset|drop|destroy>
################################################################################

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
SHARED="${PROJECT_ROOT}/node_modules/platform-db/src/scripts/sh/local-db.sh"

[ -x "${SHARED}" ] || { echo "local-db: platform-db is not installed (run npm install)" >&2; exit 1; }
cd "${PROJECT_ROOT}"
exec "${SHARED}" "$@"
