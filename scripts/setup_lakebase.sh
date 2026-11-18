#!/bin/bash
# One-time Lakebase setup (run after the first job run created the gold tables).
set -e
P=${PROFILE:-jp-fsi}; CAT=jp_fsi_catalog; SCH=agency_visit_prep; PROJ=agency-visit-prep
databricks postgres create-project $PROJ --json '{"spec": {"display_name": "代理店訪問準備 (agency-visit-prep)"}}' -p $P || true
# NOTE: no CREATE CATALOG on this metastore, so synced tables live in the regular UC catalog (works fine).
databricks postgres create-synced-table $CAT.$SCH.lb_visit_priority --no-wait -p $P --json '{"spec":{"source_table_full_name":"'$CAT.$SCH'.gold_visit_priority","primary_key_columns":["agency_id"],"scheduling_policy":"TRIGGERED","branch":"projects/'$PROJ'/branches/production","postgres_database":"databricks_postgres","create_database_objects_if_missing":true,"new_pipeline_spec":{"storage_catalog":"'$CAT'","storage_schema":"'$SCH'"}}}'
databricks postgres create-synced-table $CAT.$SCH.lb_agency_monthly_kpi --no-wait -p $P --json '{"spec":{"source_table_full_name":"'$CAT.$SCH'.gold_agency_monthly_kpi","primary_key_columns":["agency_id","month"],"scheduling_policy":"SNAPSHOT","branch":"projects/'$PROJ'/branches/production","postgres_database":"databricks_postgres","create_database_objects_if_missing":true,"new_pipeline_spec":{"storage_catalog":"'$CAT'","storage_schema":"'$SCH'"}}}'
# After (re)creating a synced table, re-grant the app SP. The synced tables are owned by databricks_writer_*,
# and ALTER DEFAULT PRIVILEGES on that role is not allowed, so this GRANT is needed every time.
#   APP_SP=$(databricks apps get agency-visit-prep -p $P -o json | jq -r .service_principal_client_id)
#   scripts/lb_psql.sh "GRANT SELECT ON ALL TABLES IN SCHEMA $SCH TO \"$APP_SP\";"
