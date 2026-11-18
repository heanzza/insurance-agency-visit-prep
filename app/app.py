"""代理店訪問準備アプリ (Databricks App / FastAPI)

- 読む: Lakebase の同期テーブル(訪問優先度リスト・月次KPI)  ← Delta Gold から同期
- 書く: Lakebase のアプリ用スキーマ app_ops(訪問プラン・訪問結果)
- 生成: Foundation Model API(Claude)で訪問プランを作る
- 質問: Genie Agent(Conversation API)
"""
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agency-visit")

w = WorkspaceClient()
LB_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "projects/agency-visit-prep/branches/production/endpoints/primary")
PG_SCHEMA = os.environ.get("SYNC_SCHEMA", "agency_visit_prep")
LLM = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
GENIE_SPACE = os.environ.get("GENIE_SPACE_ID", "")
APP_SCHEMA = "app_ops"

# ---------------------------------------------------------------- Lakebase
_tok = {"value": None, "exp": 0.0}
_host = {"value": os.environ.get("PGHOST")}


def _pg_password() -> str:
    if time.time() > _tok["exp"] - 300:
        r = w.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": LB_ENDPOINT})
        _tok["value"], _tok["exp"] = r["token"], time.time() + 3000
    return _tok["value"]


def _pg_host() -> str:
    if not _host["value"]:
        _host["value"] = w.api_client.do("GET", f"/api/2.0/postgres/{LB_ENDPOINT}")["status"]["hosts"]["host"]
    return _host["value"]


def _pg_user() -> str:
    return os.environ.get("PGUSER") or w.current_user.me().user_name


class _OAuthConnection(psycopg.Connection):
    """Inject a fresh Lakebase OAuth token whenever the pool opens a new physical connection."""
    @classmethod
    def connect(cls, conninfo: str = "", **kwargs):
        kwargs["password"] = _pg_password()
        return super().connect(conninfo, **kwargs)


_pool: ConnectionPool | None = None


def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            kwargs={"host": _pg_host(), "dbname": os.environ.get("PGDATABASE", "databricks_postgres"), "user": _pg_user(),
                    "sslmode": "require", "row_factory": dict_row, "connect_timeout": 15},
            connection_class=_OAuthConnection, min_size=1, max_size=8, max_lifetime=2400, open=True)
    return _pool


@contextmanager
def pg():
    with _get_pool().connection() as conn:  # commits on success, rolls back on error
        yield conn


def init_app_schema():
    with pg() as c:
        c.execute(f"CREATE SCHEMA IF NOT EXISTS {APP_SCHEMA}")
        c.execute(f"""CREATE TABLE IF NOT EXISTS {APP_SCHEMA}.visit_plans (
            plan_id BIGSERIAL PRIMARY KEY, agency_id TEXT NOT NULL, created_by TEXT, created_at TIMESTAMPTZ DEFAULT now(),
            visit_date DATE, status TEXT DEFAULT '予定', model TEXT, generation_ms INT, plan JSONB NOT NULL)""")
        c.execute(f"""CREATE TABLE IF NOT EXISTS {APP_SCHEMA}.visit_outcomes (
            outcome_id BIGSERIAL PRIMARY KEY, plan_id BIGINT REFERENCES {APP_SCHEMA}.visit_plans(plan_id), agency_id TEXT NOT NULL,
            created_by TEXT, created_at TIMESTAMPTZ DEFAULT now(), outcome TEXT, next_action TEXT, memo TEXT)""")
        c.execute(f"CREATE INDEX IF NOT EXISTS visit_plans_agency ON {APP_SCHEMA}.visit_plans(agency_id, created_at DESC)")


def jsonable(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def rows(sql, params=()):
    with pg() as c:
        return [{k: jsonable(v) for k, v in r.items()} for r in c.execute(sql, params).fetchall()]


# ---------------------------------------------------------------- app
app = FastAPI(title="代理店訪問準備")


@app.on_event("startup")
def _startup():
    try:
        init_app_schema()
        log.info("app schema ready (genie space=%s, llm=%s)", GENIE_SPACE or "NOT SET", LLM)
    except Exception as e:  # keep serving even if Lakebase is waking up
        log.exception("init failed: %s", e)


def user_of(req: Request) -> str:
    return req.headers.get("X-Forwarded-Email") or req.headers.get("X-Forwarded-User") or "local-user"


@app.get("/api/me")
def me(req: Request):
    return {"user": user_of(req), "genie_space_id": GENIE_SPACE, "llm": LLM}


@app.get("/api/reps")
def reps():
    return rows(f"""SELECT rep_id, rep_name, branch, count(*) AS agencies,
                           sum(CASE WHEN risk_level = '高' THEN 1 ELSE 0 END) AS high_risk,
                           sum(premium_at_risk_yen) AS premium_at_risk_yen
                    FROM {PG_SCHEMA}.lb_visit_priority GROUP BY rep_id, rep_name, branch ORDER BY rep_id""")


@app.get("/api/agencies")
def agencies(rep_id: str | None = None):
    t0 = time.perf_counter()
    sql = f"""SELECT agency_id, agency_name, channel_type, prefecture, rep_id, rep_name, branch, risk_score, risk_level,
                     premium_at_risk_yen, priority_rank, rep_priority_rank, fytd_yoy, fytd_achievement_rate,
                     proposals_3m_change, days_since_last_visit, staff_departures_6m, risk_drivers, data_as_of
              FROM {PG_SCHEMA}.lb_visit_priority {"WHERE rep_id = %s" if rep_id else ""} ORDER BY priority_rank"""
    data = rows(sql, (rep_id,) if rep_id else ())
    return {"items": data, "latency_ms": round((time.perf_counter() - t0) * 1000, 1)}


@app.get("/api/agencies/{agency_id}")
def agency(agency_id: str):
    t0 = time.perf_counter()
    a = rows(f"SELECT * FROM {PG_SCHEMA}.lb_visit_priority WHERE agency_id = %s", (agency_id,))
    if not a:
        raise HTTPException(404, "agency not found")
    kpi = rows(f"""SELECT month, premium_yen, new_policies, proposals, visits, staff_departures, lapses, target_premium_yen, achievement_rate
                   FROM {PG_SCHEMA}.lb_agency_monthly_kpi WHERE agency_id = %s ORDER BY month DESC LIMIT 12""", (agency_id,))
    plans = rows(f"""SELECT plan_id, created_by, created_at, visit_date, status, plan FROM {APP_SCHEMA}.visit_plans
                     WHERE agency_id = %s ORDER BY created_at DESC LIMIT 5""", (agency_id,))
    outcomes = rows(f"""SELECT outcome_id, plan_id, created_by, created_at, outcome, next_action, memo FROM {APP_SCHEMA}.visit_outcomes
                        WHERE agency_id = %s ORDER BY created_at DESC LIMIT 5""", (agency_id,))
    return {"agency": a[0], "kpi": list(reversed(kpi)), "plans": plans, "outcomes": outcomes,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1)}


PLAN_PROMPT = """あなたは生命保険会社「ひなた生命」の代理店営業(営業担当)のベテランマネージャーです。
担当者が明日この代理店を訪問します。以下のデータだけを根拠に、訪問プランを作ってください。データにない事実を作らないこと。
金額は万円で書く。数字を引用するときは根拠のデータ項目が分かるように書く。

出力は次の JSON だけ(前後に文章やコードフェンスを付けない):
{
  "headline": "この訪問の目的を1文(40字以内)",
  "situation": ["現状を3点。挙績・前年比・目標達成率・設計書の動きなど"],
  "risks": [{"point": "リスク", "evidence": "根拠の数字やメモ"}],
  "opportunities": [{"point": "機会", "evidence": "根拠"}],
  "questions": ["当日確認すべき質問を3つまで"],
  "agenda": [{"minutes": 10, "topic": "議題"}],
  "proposals": ["提案・支援策を2〜3個(研修、商品勉強会、販促、体制支援など)"],
  "next_actions": [{"action": "次回までの行動", "owner": "営業担当 or 代理店", "due": "期限の目安"}]
}
アジェンダの合計は60分以内。

# 代理店データ
"""


class PlanReq(BaseModel):
    agency_id: str


def _extract_json(text: str) -> dict:
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        raise ValueError("no json")
    return json.loads(m.group(0))


@app.post("/api/plan")
def generate_plan(body: PlanReq):
    d = agency(body.agency_id)
    a = d["agency"]
    ctx = {k: a.get(k) for k in ["agency_name", "channel_type", "prefecture", "established_year", "rep_name", "data_as_of", "fiscal_year",
                                 "fytd_premium_yen", "prior_fytd_premium_yen", "fytd_yoy", "fytd_target_yen", "fytd_achievement_rate",
                                 "premium_last3m_yen", "premium_prev3m_yen", "premium_3m_change", "proposals_last3m", "proposals_prev3m",
                                 "proposals_3m_change", "visits_last3m", "last_visit_date", "days_since_last_visit", "staff_departures_6m",
                                 "early_lapse_rate", "pwaiver_rate_fytd", "corp_share_fytd", "risk_score", "risk_level",
                                 "premium_at_risk_yen", "risk_drivers", "note_summary", "staff_risk_notes_6m", "competitor_notes_6m",
                                 "lapse_complaint_notes_6m", "opportunity_notes_6m"]}
    ctx["monthly_kpi_last6m"] = d["kpi"][-6:]
    ctx["previous_visit_outcomes"] = d["outcomes"][:3]
    t0 = time.perf_counter()
    resp = w.api_client.do("POST", f"/serving-endpoints/{LLM}/invocations", body={
        "messages": [{"role": "user", "content": PLAN_PROMPT + json.dumps(ctx, ensure_ascii=False, default=str)}],
        "max_tokens": 2000, "temperature": 0.2})
    ms = int((time.perf_counter() - t0) * 1000)
    content = resp["choices"][0]["message"]["content"]
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    try:
        plan = _extract_json(content)
    except Exception:
        plan = {"headline": "訪問プラン(整形できなかったため原文を表示)", "raw": content}
    return {"agency_id": body.agency_id, "plan": plan, "model": LLM, "generation_ms": ms,
            "usage": resp.get("usage")}


class SavePlanReq(BaseModel):
    agency_id: str
    plan: dict
    visit_date: str | None = None
    model: str | None = None
    generation_ms: int | None = None


@app.post("/api/plans")
def save_plan(body: SavePlanReq, req: Request):
    with pg() as c:
        r = c.execute(f"""INSERT INTO {APP_SCHEMA}.visit_plans (agency_id, created_by, visit_date, model, generation_ms, plan)
                          VALUES (%s, %s, %s, %s, %s, %s) RETURNING plan_id, created_at""",
                      (body.agency_id, user_of(req), body.visit_date or None, body.model, body.generation_ms,
                       json.dumps(body.plan, ensure_ascii=False))).fetchone()
    return {k: jsonable(v) for k, v in r.items()}


class OutcomeReq(BaseModel):
    agency_id: str
    plan_id: int | None = None
    outcome: str
    next_action: str | None = None
    memo: str | None = None


@app.post("/api/outcomes")
def save_outcome(body: OutcomeReq, req: Request):
    with pg() as c:
        r = c.execute(f"""INSERT INTO {APP_SCHEMA}.visit_outcomes (plan_id, agency_id, created_by, outcome, next_action, memo)
                          VALUES (%s, %s, %s, %s, %s, %s) RETURNING outcome_id, created_at""",
                      (body.plan_id, body.agency_id, user_of(req), body.outcome, body.next_action, body.memo)).fetchone()
        if body.plan_id:
            c.execute(f"UPDATE {APP_SCHEMA}.visit_plans SET status = '訪問済' WHERE plan_id = %s", (body.plan_id,))
    return {k: jsonable(v) for k, v in r.items()}


@app.get("/api/activity")
def activity():
    """アプリの利用状況(Lakebase に書いた運用データ)"""
    return rows(f"""SELECT p.plan_id, p.agency_id, v.agency_name, p.created_by, p.created_at, p.status, p.generation_ms,
                           p.plan->>'headline' AS headline
                    FROM {APP_SCHEMA}.visit_plans p LEFT JOIN {PG_SCHEMA}.lb_visit_priority v USING (agency_id)
                    ORDER BY p.created_at DESC LIMIT 20""")


# ---------------------------------------------------------------- Genie
class GenieReq(BaseModel):
    question: str
    conversation_id: str | None = None


@app.post("/api/genie")
def genie(body: GenieReq):
    if not GENIE_SPACE:
        raise HTTPException(500, "GENIE_SPACE_ID not set")
    base = f"/api/2.0/genie/spaces/{GENIE_SPACE}"
    if body.conversation_id:
        m = w.api_client.do("POST", f"{base}/conversations/{body.conversation_id}/messages", body={"content": body.question})
        conv, mid = body.conversation_id, m.get("message_id") or m.get("id")
    else:
        m = w.api_client.do("POST", f"{base}/start-conversation", body={"content": body.question})
        conv, mid = m["conversation_id"], m["message_id"]
    t0 = time.time()
    while True:
        msg = w.api_client.do("GET", f"{base}/conversations/{conv}/messages/{mid}")
        if msg["status"] in ("COMPLETED", "FAILED", "CANCELLED") or time.time() - t0 > 120:
            break
        time.sleep(2)
    out = {"conversation_id": conv, "status": msg["status"], "text": [], "sql": None, "columns": [], "rows": []}
    for att in msg.get("attachments") or []:
        if "text" in att:
            out["text"].append(att["text"].get("content", ""))
        if "query" in att:
            out["sql"] = att["query"].get("query")
            if att["query"].get("description"):
                out["text"].insert(0, att["query"]["description"])
            res = w.api_client.do("GET", f"{base}/conversations/{conv}/messages/{mid}/attachments/{att['attachment_id']}/query-result")
            sr = res.get("statement_response", {})
            out["columns"] = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            out["rows"] = sr.get("result", {}).get("data_array", [])[:50]
    return out


@app.get("/api/health")
def health():
    t0 = time.perf_counter()
    n = rows(f"SELECT count(*) AS n FROM {PG_SCHEMA}.lb_visit_priority")[0]["n"]
    return {"ok": True, "agencies": n, "lakebase_ms": round((time.perf_counter() - t0) * 1000, 1)}


@app.exception_handler(Exception)
async def _err(_, exc: Exception):
    log.exception("error")
    return JSONResponse(status_code=500, content={"detail": str(exc)})


app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))
