"""Create / update the Genie Agent "代理店営業アシスタント" for the agency visit prep demo.

usage: WAREHOUSE_ID=<id> [CATALOG=.. SCHEMA=..] python3 scripts/build_genie.py            # create (or update if genie/space_id.txt exists)
"""
import json, os, subprocess, sys, uuid, hashlib

PROFILE = os.environ.get("PROFILE", "jp-fsi")
WH = os.environ.get("WAREHOUSE_ID") or sys.exit("set WAREHOUSE_ID (SQL warehouse id)")
FQ = f'{os.environ.get("CATALOG", "jp_fsi_catalog")}.{os.environ.get("SCHEMA", "agency_visit_prep")}'
HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "genie")
os.makedirs(OUT, exist_ok=True)


def sid(s: str) -> str:  # deterministic 32-hex ids so re-runs produce stable payloads
    return hashlib.md5(s.encode()).hexdigest()


TABLES = {
    "gold_visit_priority": [
        ("agency_name", ["代理店名"], ["代理店", "店名"], True),
        ("rep_name", ["担当営業の氏名"], ["担当", "営業担当", "担当者"], True),
        ("risk_level", ["リスク区分 高/中/低"], ["リスク"], True),
        ("branch", ["営業部"], ["営業部", "支社"], True),
        ("channel_type", ["代理店の形態"], ["形態", "チャネル"], True),
        ("prefecture", ["都道府県"], ["地域", "エリア"], True),
        ("fytd_premium_yen", ["今年度累計の挙績(円)"], ["今年度の挙績", "年度累計"], False),
        ("fytd_yoy", ["前年比(今年度累計 vs 前年度同期間)"], ["前年比", "前年同期比", "YoY"], False),
        ("premium_at_risk_yen", ["リスクにさらされた挙績(円)"], ["リスク金額", "守るべき挙績"], False),
        ("proposals_3m_change", ["設計書作成数の変化率(直近3か月 vs その前3か月)"], ["設計書の減少", "設計書の伸び"], False),
    ],
    "gold_agency_monthly_kpi": [],
    "gold_visit_note_signals": [("signal_category", ["AIが分類した訪問メモのシグナル"], ["シグナル", "兆候"], True)],
    "silver_policies": [("customer_type", ["個人 / 法人"], ["契約者区分"], True)],
    "silver_products": [("product_category", ["商品カテゴリ"], ["カテゴリ", "商品分類"], True),
                        ("product_name", ["商品名"], ["商品"], True)],
    "silver_sales_reps": [],
    "silver_lapses": [("reason", ["解約・失効の理由"], ["解約理由"], True)],
}


def table_entry(name, cols):
    t = {"identifier": f"{FQ}.{name}"}
    if cols:
        t["column_configs"] = sorted([
            {"column_name": c, "description": d, "synonyms": syn,
             **({"enable_format_assistance": True, "enable_entity_matching": True} if em else {})}
            for c, d, syn, em in cols], key=lambda x: x["column_name"])
    return t


INSTRUCTIONS = f"""
あなたは架空の生命保険会社「ひなた生命」の代理店営業(営業担当)を支えるアシスタントです。回答は日本語で、金額は「万円」単位(小数1桁)で表示してください。

## 用語と定義
- 挙績 = 新契約の年換算保険料の合計(premium_yen / fytd_premium_yen など)。
- 年度は4月始まり(2026年度 = 2026年4月〜2027年3月)。
- 前年比 = 今年度累計(4月〜基準月)と前年度の同じ期間の比較。gold_visit_priority.fytd_yoy を使う。暦年で比べない。
- 基準日 = gold_visit_priority.data_as_of(2026-10-31)。「今月」「先月」「直近」は current_date() ではなくこの基準日を基準にする。「先月」= 2026年10月。
- 挙績減少リスク(risk_score) = 次の3か月で挙績が直近3か月比 20%以上落ちる確率(MLモデルの予測)。risk_level は 高(0.5以上)/中(0.25以上)/低。
- 訪問優先度 = premium_at_risk_yen(リスク × 直近3か月の挙績)の大きい順。priority_rank が全社順位、rep_priority_rank が担当内順位。
- 設計書作成数(proposals)は契約の先行指標。設計書が減ると1〜2か月後に新契約が減る。
- P免付加率 = P免を付けた契約 / P免を付けられる商品の契約。分母は silver_products.p_waiver_available = true の契約だけ(gold_agency_monthly_kpi の pwaiver_attached_policies / pwaiver_eligible_policies を合計して割る)。
- 早期解約率 = 契約から13か月以内に解約・失効した割合(gold_visit_priority.early_lapse_rate)。
- 法人比率は挙績ベース(corp_share_fytd)。

## テーブルの使い分け
- 代理店ごとの現状・リスク・優先度・メモ要約 → gold_visit_priority(1代理店1行)
- 月次の推移 → gold_agency_monthly_kpi(代理店 × 月)。代理店名は gold_visit_priority と agency_id で結合する
- 訪問メモの中身・兆候 → gold_visit_note_signals(signal_category で絞る)
- 商品別・契約者区分別の分析 → silver_policies と silver_products(product_id で結合)
- 解約の理由 → silver_lapses と silver_policies(policy_id で結合)

## 回答のしかた
- 代理店名で聞かれたら agency_name を部分一致(LIKE '%…%')で探す。
- 担当者名で聞かれたら rep_name を部分一致で探す。
- 一覧は多くても上位10件。理由を聞かれたら risk_drivers と note_summary を引用する。
- 「訪問プラン」「訪問準備」を頼まれたら、その代理店の gold_visit_priority の1行と直近6か月の gold_agency_monthly_kpi を確認し、
  1) 現状(挙績・前年比・目標達成率) 2) リスクと根拠 3) 当日確認すべき質問(3つまで) 4) 提案アジェンダ の順で答える。
""".strip()

EXAMPLES = [
    ("訪問優先度が高い代理店トップ10は？",
     f"""SELECT priority_rank, agency_name, rep_name, risk_level, ROUND(risk_score, 2) AS risk_score,
       ROUND(premium_at_risk_yen / 10000, 1) AS premium_at_risk_man_yen, risk_drivers
FROM {FQ}.gold_visit_priority ORDER BY priority_rank LIMIT 10"""),
    ("担当営業ごとに、リスク高の代理店数と守るべき挙績を出して",
     f"""SELECT rep_name, branch, count(*) AS agencies, count_if(risk_level = '高') AS high_risk_agencies,
       ROUND(sum(premium_at_risk_yen) / 10000, 1) AS premium_at_risk_man_yen
FROM {FQ}.gold_visit_priority GROUP BY rep_name, branch ORDER BY premium_at_risk_man_yen DESC"""),
    ("前年比で伸びている代理店は？",
     f"""SELECT agency_name, rep_name, ROUND(fytd_premium_yen / 10000, 1) AS fytd_man_yen,
       ROUND(prior_fytd_premium_yen / 10000, 1) AS prior_fytd_man_yen, ROUND(fytd_yoy * 100, 1) AS yoy_pct
FROM {FQ}.gold_visit_priority WHERE prior_fytd_premium_yen > 0 ORDER BY fytd_yoy DESC LIMIT 10"""),
    ("直近の訪問メモで募集人の退職や人手不足の話が出た代理店は？",
     f"""SELECT p.agency_name, p.rep_name, count(*) AS notes, max(s.visit_date) AS last_mentioned, max_by(s.note_text, s.visit_date) AS latest_note
FROM {FQ}.gold_visit_note_signals s JOIN {FQ}.gold_visit_priority p USING (agency_id)
WHERE s.signal_category = '募集人の退職・人手不足'
GROUP BY p.agency_name, p.rep_name ORDER BY last_mentioned DESC"""),
    ("P免付加率が低い代理店は？",
     f"""SELECT p.agency_name, sum(k.pwaiver_attached_policies) AS attached, sum(k.pwaiver_eligible_policies) AS eligible,
       ROUND(try_divide(sum(k.pwaiver_attached_policies), sum(k.pwaiver_eligible_policies)) * 100, 1) AS pwaiver_rate_pct
FROM {FQ}.gold_agency_monthly_kpi k JOIN {FQ}.gold_visit_priority p USING (agency_id)
WHERE k.fiscal_year = 2026
GROUP BY p.agency_name HAVING sum(k.pwaiver_eligible_policies) >= 20 ORDER BY pwaiver_rate_pct LIMIT 10"""),
    ("さくら保険サービスの直近6か月の挙績・設計書・訪問の推移",
     f"""SELECT k.month, ROUND(k.premium_yen / 10000, 1) AS premium_man_yen, k.new_policies, k.proposals, k.visits, k.staff_departures,
       ROUND(k.achievement_rate * 100, 1) AS achievement_pct
FROM {FQ}.gold_agency_monthly_kpi k JOIN {FQ}.gold_visit_priority p USING (agency_id)
WHERE p.agency_name LIKE '%さくら保険サービス%' AND k.month > add_months(p.data_as_of, -6)
ORDER BY k.month"""),
]

SAMPLES = ["今週訪問すべき代理店トップ5とその理由は？", "小林さんの担当代理店で、リスクが高いのはどこ？",
           "設計書作成数が大きく減っている代理店は？", "解約の理由で一番多いものは？", "さくら保険サービスの訪問プランを作って"]

BENCH = [
    ("リスク区分が高の代理店は何社？", f"SELECT count(*) FROM {FQ}.gold_visit_priority WHERE risk_level = '高'"),
    ("今年度の全社の挙績合計は？", f"SELECT sum(fytd_premium_yen) FROM {FQ}.gold_visit_priority"),
    ("訪問優先度1位の代理店は？", f"SELECT agency_name FROM {FQ}.gold_visit_priority WHERE priority_rank = 1"),
    ("2026年10月に設計書を最も多く作った代理店は？",
     f"SELECT p.agency_name, k.proposals FROM {FQ}.gold_agency_monthly_kpi k JOIN {FQ}.gold_visit_priority p USING (agency_id) WHERE k.month = '2026-10-01' ORDER BY k.proposals DESC LIMIT 1"),
    ("最後の訪問から60日以上たっている代理店は？",
     f"SELECT agency_name, rep_name, days_since_last_visit FROM {FQ}.gold_visit_priority WHERE days_since_last_visit >= 60 ORDER BY days_since_last_visit DESC"),
]

space = {
    "version": 2,
    "config": {"sample_questions": sorted([{"id": sid("s" + q), "question": [q]} for q in SAMPLES], key=lambda x: x["id"])},
    "data_sources": {"tables": sorted([table_entry(n, c) for n, c in TABLES.items()], key=lambda x: x["identifier"])},
    "instructions": {
        "text_instructions": [{"id": sid("instr"), "content": [INSTRUCTIONS]}],
        "example_question_sqls": sorted([{"id": sid("e" + q), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"]),
    },
    "benchmarks": {"questions": sorted([{"id": sid("b" + q), "question": [q], "answer": [{"format": "SQL", "content": [s]}]} for q, s in BENCH],
                                       key=lambda x: x["id"])},
}
ser = json.dumps(space, ensure_ascii=False)
open(os.path.join(OUT, "serialized_space.json"), "w").write(json.dumps(space, ensure_ascii=False, indent=2))

title = "代理店営業アシスタント(ひなた生命・訪問準備)"
desc = "代理店の挙績・設計書・訪問・解約・MLリスクスコア・訪問メモのAI分類を自然言語で聞ける。架空の合成データ。"
idf = os.path.join(OUT, "space_id.txt")
if os.path.exists(idf):
    space_id = open(idf).read().strip()
    body = {"serialized_space": ser, "title": title, "description": desc, "warehouse_id": WH}
    r = subprocess.run(["databricks", "genie", "update-space", space_id, "--json", json.dumps(body, ensure_ascii=False), "-p", PROFILE, "-o", "json"],
                       capture_output=True, text=True)
else:
    r = subprocess.run(["databricks", "genie", "create-space", WH, ser, "--title", title, "--description", desc, "-p", PROFILE, "-o", "json"],
                       capture_output=True, text=True)
if r.returncode:
    sys.exit(r.stderr)
res = json.loads(r.stdout)
space_id = res["space_id"]
open(idf, "w").write(space_id)
print("space_id:", space_id)
