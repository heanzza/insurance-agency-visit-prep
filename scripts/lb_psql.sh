#!/bin/bash
# usage: scripts/lb_psql.sh "SQL"   -> runs SQL on the Lakebase project agency-visit-prep (production branch) as the CLI user
EP=projects/agency-visit-prep/branches/production/endpoints/primary
HOST=$(databricks postgres get-endpoint $EP -p ${PROFILE:-jp-fsi} -o json | jq -r .status.hosts.host)
TOKEN=$(databricks postgres generate-database-credential $EP -p ${PROFILE:-jp-fsi} -o json | jq -r .token)
USER_NAME=$(databricks current-user me -p ${PROFILE:-jp-fsi} -o json | jq -r .userName)
PGPASSWORD="$TOKEN" psql "host=$HOST user=$USER_NAME dbname=databricks_postgres sslmode=require" -c "$1"
