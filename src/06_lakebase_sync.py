# Databricks notebook source
# MAGIC %md
# MAGIC # 06. Lakebase 同期(Delta → Postgres)
# MAGIC Gold の2表を Lakebase(サーバーレス Postgres)に同期する。アプリはこの Postgres を読む(1代理店の取得で 1ms 未満)。
# MAGIC
# MAGIC | 同期テーブル (UC) | 同期元 (Delta) | モード |
# MAGIC |---|---|---|
# MAGIC | `lb_visit_priority` | `gold_visit_priority` | TRIGGERED(Change Data Feed で差分だけ反映) |
# MAGIC | `lb_agency_monthly_kpi` | `gold_agency_monthly_kpi` | SNAPSHOT |
# MAGIC
# MAGIC 同期テーブルは `scripts/setup_lakebase.sh` で一度だけ作る。このノートブックは毎回の更新を起動し、終わるまで待つ。

# COMMAND ----------

dbutils.widgets.text("catalog", "jp_fsi_catalog")
dbutils.widgets.text("schema", "agency_visit_prep")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

import time
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

def synced(name):
    return w.api_client.do("GET", f"/api/2.0/postgres/synced_tables/{C}.{S}.{name}")

def wait(pid, uid, timeout_s=900):
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        u = w.pipelines.get_update(pipeline_id=pid, update_id=uid).update
        if u.state.value in ("COMPLETED", "FAILED", "CANCELED"):
            return u.state.value
        time.sleep(15)
    return "TIMEOUT"

results = {}
for name in ["lb_visit_priority", "lb_agency_monthly_kpi"]:
    pid = synced(name)["status"]["pipeline_id"]
    # 実行中の更新があれば終わるまで待つ
    for u in w.pipelines.list_updates(pipeline_id=pid, max_results=3).updates or []:
        if u.state.value not in ("COMPLETED", "FAILED", "CANCELED"):
            print(f"{name}: waiting for active update {u.update_id} -> {wait(pid, u.update_id)}")
    last = w.pipelines.list_updates(pipeline_id=pid, max_results=1).updates
    full = bool(last) and last[0].state.value == "FAILED"   # 前回失敗していたらフルリフレッシュで立て直す
    upd = w.pipelines.start_update(pipeline_id=pid, full_refresh=full)
    print(f"{name}: started update {upd.update_id} on pipeline {pid} (full_refresh={full})")
    results[name] = (pid, upd.update_id)

for name, (pid, uid) in results.items():
    state = wait(pid, uid)
    st = synced(name)["status"]
    print(f"{name}: update={state} state={st['detailed_state']} last_sync={st.get('last_sync', {}).get('delta_table_sync_info', st.get('last_sync'))}")
    assert state == "COMPLETED", f"{name} sync failed"

# COMMAND ----------

# MAGIC %md ### Postgres 側の確認(OAuth トークンで接続して件数と応答時間を見る)

# COMMAND ----------

# MAGIC %pip install -q "psycopg[binary]>=3.1"

# COMMAND ----------

import time, uuid, psycopg
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
EP = "projects/agency-visit-prep/branches/production/endpoints/primary"
ep = w.api_client.do("GET", f"/api/2.0/postgres/{EP}")
host = ep["status"]["hosts"]["host"]
token = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": EP})["token"]
me = w.current_user.me().user_name
with psycopg.connect(host=host, dbname="databricks_postgres", user=me, password=token, sslmode="require") as conn:
    cur = conn.cursor()
    for t in ["lb_visit_priority", "lb_agency_monthly_kpi"]:
        cur.execute(f"SELECT count(*) FROM {S}.{t}")
        print(t, "rows in Postgres:", cur.fetchone()[0])
    lat = []
    for aid in ["A0057", "A0097", "A0124", "A0001", "A0150"]:
        t0 = time.perf_counter()
        cur.execute(f"SELECT agency_name, risk_level, priority_rank FROM {S}.lb_visit_priority WHERE agency_id = %s", (aid,))
        row = cur.fetchone()
        lat.append((time.perf_counter() - t0) * 1000)
        print(aid, row)
    print(f"point lookup latency (ms): avg={sum(lat)/len(lat):.1f} max={max(lat):.1f}")
