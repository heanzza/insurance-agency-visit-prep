#!/bin/bash
# Attach app resources, upload app/ and deploy the Databricks App. Run from repo root.
#   PROFILE=<cli profile> WAREHOUSE_ID=<sql warehouse id> scripts/deploy_app.sh
# GENIE_SPACE_ID defaults to genie/space_id.txt (written by scripts/build_genie.py).
set -e
P=${PROFILE:-jp-fsi}; APP=agency-visit-prep
: "${WAREHOUSE_ID:?set WAREHOUSE_ID}"
export WAREHOUSE_ID GENIE_SPACE_ID=${GENIE_SPACE_ID:-$(cat genie/space_id.txt)}
databricks apps get $APP -p $P >/dev/null 2>&1 || databricks apps create $APP -p $P >/dev/null
envsubst < app/resources.json | jq -c .app > /tmp/$APP-resources.json
databricks apps update $APP --json @/tmp/$APP-resources.json -p $P >/dev/null
ME=$(databricks current-user me -p $P -o json | jq -r .userName)
SRC=/Workspace/Users/$ME/apps/$APP
databricks workspace import-dir app $SRC --overwrite -p $P >/dev/null
databricks apps deploy $APP --source-code-path $SRC -p $P -o json | jq -c '{status: .status.state, msg: .status.message}'
