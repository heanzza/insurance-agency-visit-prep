"""Ask the Genie Agent questions via the Conversation API and write a text transcript (question, SQL, answer, rows).

usage: python3 scripts/ask_genie.py [question ...]   -> evidence/genie_conversations.md
"""
import json, os, subprocess, sys, time

PROFILE = os.environ.get("PROFILE", "jp-fsi")
HERE = os.path.dirname(os.path.abspath(__file__))
SPACE = open(os.path.join(HERE, "..", "genie", "space_id.txt")).read().strip()
DEFAULT_QS = [
    "今週訪問すべき代理店トップ5とその理由は？",
    "小林さんの担当代理店で、リスクが高いのはどこ？",
    "設計書作成数が大きく減っている代理店は？",
    "直近の訪問メモで募集人の退職や人手不足の話が出た代理店は？",
    "前年比で伸びている代理店トップ5は？",
    "解約の理由で一番多いものは？",
    "P免付加率が低い代理店は？",
    "最後の訪問から60日以上たっている代理店は？",
]


def api(method, path, body=None):
    cmd = ["databricks", "api", method, path, "-p", PROFILE]
    if body is not None:
        cmd += ["--json", json.dumps(body, ensure_ascii=False)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(r.stderr)
    return json.loads(r.stdout or "{}")


def ask(q):
    t0 = time.time()
    m = api("post", f"/api/2.0/genie/spaces/{SPACE}/start-conversation", {"content": q})
    conv, mid = m["conversation_id"], m["message_id"]
    while True:
        msg = api("get", f"/api/2.0/genie/spaces/{SPACE}/conversations/{conv}/messages/{mid}")
        if msg["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(3)
    out = {"question": q, "status": msg["status"], "seconds": round(time.time() - t0, 1), "text": [], "sql": None, "rows": [], "cols": []}
    for a in msg.get("attachments", []) or []:
        if "text" in a:
            out["text"].append(a["text"]["content"])
        if "query" in a:
            out["sql"] = a["query"]["query"]
            out["text"].append(a["query"].get("description", ""))
            res = api("get", f"/api/2.0/genie/spaces/{SPACE}/conversations/{conv}/messages/{mid}/attachments/{a['attachment_id']}/query-result")
            sr = res.get("statement_response", {})
            out["cols"] = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            out["rows"] = sr.get("result", {}).get("data_array", [])[:10]
    return out


def to_md(r):
    s = [f"## Q. {r['question']}", "", f"- status: `{r['status']}` / {r['seconds']} 秒", ""]
    for t in r["text"]:
        if t:
            s += [f"> {line}" for line in t.splitlines()] + [""]
    if r["sql"]:
        s += ["```sql", r["sql"].strip(), "```", ""]
    if r["cols"]:
        s += ["| " + " | ".join(r["cols"]) + " |", "|" + "---|" * len(r["cols"])]
        s += ["| " + " | ".join("" if v is None else str(v).replace("|", "/").replace("\n", " ") for v in row) + " |" for row in r["rows"]]
        s += [""]
    return "\n".join(s)


if __name__ == "__main__":
    qs = sys.argv[1:] or DEFAULT_QS
    results = [ask(q) for q in qs]
    md = [f"# Genie Agent 実行ログ(Conversation API)", "", f"- 実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}",
          f"- 成功: {sum(r['status'] == 'COMPLETED' for r in results)}/{len(results)}", ""]
    md += [to_md(r) for r in results]
    path = os.path.join(HERE, "..", "evidence", "genie_conversations.md")
    open(path, "w").write("\n".join(md))
    json.dump(results, open(path.replace(".md", ".json"), "w"), ensure_ascii=False, indent=2)
    for r in results:
        print(r["status"], r["seconds"], r["question"], "| rows:", len(r["rows"]))
