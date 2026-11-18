# Databricks notebook source
# MAGIC %md
# MAGIC # 01. 合成データ生成(Raw Landing)
# MAGIC
# MAGIC 架空の生命保険会社「ひなた生命」の **代理店チャネル** の生データを作り、Unity Catalog Volume に置く。
# MAGIC 実在の会社・代理店・人物とは関係ない。すべて乱数で作った合成データ。
# MAGIC
# MAGIC | ファイル | 形式 | 内容 |
# MAGIC |---|---|---|
# MAGIC | `master/agencies.csv` | CSV | 代理店マスタ 150社(連絡先=PII含む) |
# MAGIC | `master/sales_reps.csv` | CSV | 営業担当 15名 |
# MAGIC | `master/products.csv` | CSV | 商品 12種 |
# MAGIC | `policies/policies_YYYYMM.csv` | CSV(月次) | 新契約 |
# MAGIC | `lapses/lapses_YYYYMM.csv` | CSV(月次) | 解約・失効 |
# MAGIC | `activities/activities_YYYYMM.jsonl` | JSON Lines(月次) | 設計書作成・訪問・研修・募集人の登録/退職 |
# MAGIC | `visit_notes/visit_notes_YYYYMM.jsonl` | JSON Lines(月次) | 営業担当の訪問メモ(自由記述テキスト) |
# MAGIC | `targets/targets_FY{2024..2026}.csv` | CSV | 代理店別の月次挙績目標 |
# MAGIC
# MAGIC 月次ファイルを順に置くことで、Lakeflow の Auto Loader が「毎月届くファイル」を増分で取り込む状況を再現する。
# MAGIC
# MAGIC **仕込んだストーリー**(後で ML と Genie で見つけられるか確認する)
# MAGIC - 一部の代理店で主力募集人が退職 → **2か月前に設計書作成数が落ち** → その後に新契約が減る(先行指標)
# MAGIC - 営業担当の訪問が多い代理店ほど、設計書から契約への転換率が高い
# MAGIC - 一部の代理店は早期解約(13か月以内)が多い

# COMMAND ----------

dbutils.widgets.text("catalog", "jp_fsi_catalog")
dbutils.widgets.text("schema", "agency_visit_prep")
dbutils.widgets.text("volume", "raw")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
OUT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT '代理店営業の訪問準備デモ(合成データ)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} COMMENT '生データの着地点(CSV/JSON)。Lakeflow Auto Loaderで取り込む'")
print("output:", OUT)

# COMMAND ----------

import json, os, shutil
import numpy as np
import pandas as pd

rng = np.random.default_rng(20261118)
MONTHS = pd.period_range("2024-11", "2026-10", freq="M")  # 24か月。データの締めは 2026-10-31
N_AGENCY, N_REP = 150, 15

# ---------- 営業担当 ----------
last = ["佐藤", "鈴木", "高橋", "田中", "伊藤", "渡辺", "山本", "中村", "小林", "加藤", "吉田", "山田", "松本", "井上", "木村"]
first = ["健太", "美咲", "大輔", "彩", "翔", "麻衣", "拓也", "由佳", "直樹", "真由", "亮", "恵", "誠", "沙織", "悠"]
branches = ["東京第一営業部", "東京第二営業部", "横浜営業部", "大阪営業部", "名古屋営業部"]
reps = pd.DataFrame({
    "rep_id": [f"R{i:03d}" for i in range(1, N_REP + 1)],
    "rep_name": [f"{last[i]} {first[i]}" for i in range(N_REP)],
    "branch": [branches[i % 5] for i in range(N_REP)],
    "email": [f"rep{i:03d}@hinata-life.example" for i in range(1, N_REP + 1)],
})

# ---------- 代理店 ----------
pre = ["あおば", "みなと", "さくら", "ひかり", "やまびこ", "つばさ", "はるか", "いろは", "こもれび", "そよかぜ",
       "あかつき", "しらゆり", "ほしぞら", "わかば", "なぎさ", "かえで", "すずらん", "ことぶき", "あさひ", "ゆうなぎ",
       "みどり", "せせらぎ", "ひまわり", "まほろば", "いずみ"]
suf = ["保険サービス", "ライフプランニング", "保険企画", "インシュアランス", "保険パートナーズ", "ほけん相談室"]
names = rng.permutation([p + s for p in pre for s in suf])[:N_AGENCY]
pref_by_branch = {"東京第一営業部": ["東京都"], "東京第二営業部": ["東京都", "埼玉県", "千葉県"],
                  "横浜営業部": ["神奈川県"], "大阪営業部": ["大阪府", "兵庫県", "京都府"], "名古屋営業部": ["愛知県", "岐阜県", "静岡県"]}
channel = rng.choice(["乗合代理店", "専属代理店", "来店型ショップ", "金融機関代理店"], N_AGENCY, p=[.45, .25, .2, .1])
rep_of = np.array([f"R{(i % N_REP) + 1:03d}" for i in range(N_AGENCY)])
rep_branch = dict(zip(reps.rep_id, reps.branch))
agencies = pd.DataFrame({
    "agency_id": [f"A{i:04d}" for i in range(1, N_AGENCY + 1)],
    "agency_name": names,
    "channel_type": channel,
    "prefecture": [rng.choice(pref_by_branch[rep_branch[r]]) for r in rep_of],
    "established_year": rng.integers(1985, 2021, N_AGENCY),
    "rep_id": rep_of,
    "contact_person": [f"{rng.choice(last)} {rng.choice(first)}" for _ in range(N_AGENCY)],
    "contact_phone": [f"0{rng.integers(3,9)}-{rng.integers(1000,9999)}-{rng.integers(1000,9999)}" for _ in range(N_AGENCY)],
    "contact_email": [f"info{i:04d}@agency-{i:04d}.example" for i in range(1, N_AGENCY + 1)],
})

# ---------- 商品 ----------
products = pd.DataFrame([
    ("P01", "ひなたの終身", "終身保険", 120_000, True), ("P02", "ひなた定期プラス", "定期保険", 60_000, True),
    ("P03", "ひなた医療ネクスト", "医療保険", 45_000, True), ("P04", "ひなたがん保険", "がん保険", 40_000, True),
    ("P05", "ひなた介護一時金", "介護保険", 70_000, True), ("P06", "ひなた収入保障", "収入保障保険", 55_000, True),
    ("P07", "ひなた変額終身", "変額保険", 240_000, False), ("P08", "ひなた外貨建て終身", "外貨建て保険", 300_000, False),
    ("P09", "ひなた個人年金", "個人年金保険", 180_000, False), ("P10", "ひなた低解約返戻金型終身", "終身保険", 100_000, True),
    ("P11", "ひなた経営者定期", "法人向け定期保険", 400_000, False), ("P12", "ひなた就業不能", "就業不能保険", 50_000, True),
], columns=["product_id", "product_name", "product_category", "avg_annual_premium_yen", "p_waiver_available"])
prod_p = np.array([.12, .09, .14, .10, .05, .08, .08, .06, .06, .07, .07, .08])

# ---------- 代理店ごとの潜在パラメータ ----------
base_prop = rng.lognormal(np.log(28), 0.55, N_AGENCY)          # 月あたり設計書作成数の基準
conv = np.clip(rng.normal(0.38, 0.07, N_AGENCY), 0.18, 0.6)       # 設計書→契約の転換率
lapse_q = np.where(rng.random(N_AGENCY) < 0.12, rng.uniform(0.20, 0.30, N_AGENCY), rng.uniform(0.05, 0.10, N_AGENCY))
trend = rng.normal(0.004, 0.008, N_AGENCY)                       # 月次の自然な伸び
corp_share = np.where(rng.random(N_AGENCY) < 0.2, 0.25, 0.05)
staff0 = rng.integers(3, 25, N_AGENCY)
visit_rate = rng.uniform(0.6, 3.0, N_AGENCY)                     # 営業担当の月あたり訪問回数(平均)

# 募集人退職ショック: 各代理店 0〜2回。直近(2026-07〜09)に集中させた代理店を約20社作る(=今まさに要注意)
shock = {i: [] for i in range(N_AGENCY)}
for i in range(N_AGENCY):
    for _ in range(rng.choice([0, 1, 2], p=[.45, .4, .15])):
        shock[i].append(int(rng.integers(2, 20)))
recent = rng.choice(N_AGENCY, 22, replace=False)
for i in recent:
    shock[i].append(int(rng.integers(20, 23)))   # 2026-07〜2026-09
growth_ag = rng.choice(list(set(range(N_AGENCY)) - set(recent)), 15, replace=False)
trend[growth_ag] += 0.03

T = len(MONTHS)
prop_mult = np.ones((N_AGENCY, T))
staff = np.tile(staff0[:, None], (1, T)).astype(float)
for i in range(N_AGENCY):
    for s in shock[i]:
        sev = rng.uniform(0.35, 0.6)
        for t in range(s, T):
            rec = min(1.0, (t - s) / 9)               # 9か月かけて回復
            prop_mult[i, t] *= (1 - sev * (1 - rec))
        lost = max(1, int(staff0[i] * rng.uniform(0.15, 0.35)))
        staff[i, s:] -= lost
staff = np.maximum(staff, 1)

visits = rng.poisson(visit_rate[:, None] * np.ones((1, T)))
season = np.array([1.0, 1.15, 0.85, 1.25, 0.95, 0.9, 1.0, 1.0, 1.05, 0.95, 1.0, 1.05] * 3)[:T]  # 11月始まり
proposals = rng.poisson(base_prop[:, None] * prop_mult * season[None, :] * np.exp(trend[:, None] * np.arange(T)))
print("agencies with recent shock:", len(recent), " total proposals:", proposals.sum())

# COMMAND ----------

# ---------- 新契約(設計書の 1〜2か月後に成約、訪問が多いほど転換率UP) ----------
pol_rows, lapse_rows = [], []
pid = 0
first_names_c = ["個人", "法人"]
for i in range(N_AGENCY):
    for t in range(T):
        lag_prop = 0.6 * proposals[i, t - 1] + 0.4 * proposals[i, t - 2] if t >= 2 else proposals[i, t]
        c = conv[i] * (1 + 0.06 * min(visits[i, t - 1] if t else 1, 4))
        n = rng.poisson(lag_prop * c)
        m = MONTHS[t]
        days = rng.integers(1, m.days_in_month + 1, n)
        for d in days:
            pid += 1
            cust = "法人" if rng.random() < corp_share[i] else "個人"
            if cust == "法人":
                prod = products.iloc[rng.choice([10, 0, 6, 7])]
            else:
                prod = products.iloc[rng.choice(12, p=prod_p)]
            prem = int(round(prod.avg_annual_premium_yen * rng.lognormal(0, 0.35), -2))
            pw = bool(prod.p_waiver_available and rng.random() < 0.55)
            date = pd.Timestamp(year=m.year, month=m.month, day=int(d))
            pol_rows.append((f"PL{pid:07d}", agencies.agency_id[i], prod.product_id, date.strftime("%Y-%m-%d"),
                             prem, cust, int(rng.integers(22, 75)), "あり" if pw else "なし"))
            if rng.random() < lapse_q[i]:
                lag = int(rng.integers(1, 25))
                ld = date + pd.DateOffset(months=lag) + pd.Timedelta(days=int(rng.integers(0, 28)))
                if ld <= pd.Timestamp("2026-10-31"):
                    lapse_rows.append((f"PL{pid:07d}", ld.strftime("%Y-%m-%d"), rng.choice(["解約", "失効"], p=[.7, .3]),
                                       rng.choice(["保険料負担", "他社への乗換", "家計の見直し", "理由不明"], p=[.35, .3, .2, .15])))

# データ品質チェック用に、不正な行を少しだけ混ぜる(保険料0以下・代理店ID欠損)。Silver の Expectation で落ちることを確認する
bad = rng.choice(len(pol_rows), 60, replace=False)
for k, j in enumerate(bad):
    r = list(pol_rows[j]); r[0] = f"PLX{k:05d}"
    if k % 2: r[4] = -abs(r[4])
    else: r[1] = None
    pol_rows.append(tuple(r))
policies = pd.DataFrame(pol_rows, columns=["policy_id", "agency_id", "product_id", "contract_date",
                                           "annualized_premium_yen", "customer_type", "insured_age", "p_waiver_rider"])
lapses = pd.DataFrame(lapse_rows, columns=["policy_id", "event_date", "event_type", "reason"])
print(len(policies), "policies /", len(lapses), "lapses")

# COMMAND ----------

# ---------- 活動ログ(JSON) と 訪問メモ(テキスト) ----------
topics_ok = ["新商品「ひなた介護一時金」の勉強会を希望", "法人顧客の事業承継ニーズについて相談", "来月のセミナー集客は順調",
             "変額保険の販売資格を取る募集人が2名増えた", "既契約の保全手続きの問い合わせが多い", "医療保険の比較表を更新してほしいとの要望"]
topics_risk = ["主力募集人の{n}さんが今月末で退職予定。後任は未定", "他社の手数料改定で、他社商品の扱いを増やすと聞いた",
               "設計書の作成が先月から減っている。店長は人手不足と説明", "募集人が{n}名辞めて、来店客の対応で手一杯とのこと"]
topics_lapse = ["早期解約の苦情が2件あった。説明不足の可能性", "保険料の負担を理由とした解約相談が増えている"]
act_rows, note_rows = [], []
eid = nid = 0
for t, m in enumerate(MONTHS):
    for i in range(N_AGENCY):
        aid, rid = agencies.agency_id[i], agencies.rep_id[i]
        for _ in range(proposals[i, t]):
            eid += 1
            d = pd.Timestamp(year=m.year, month=m.month, day=int(rng.integers(1, m.days_in_month + 1)))
            act_rows.append({"event_id": f"E{eid:08d}", "agency_id": aid, "event_ts": (d + pd.Timedelta(hours=int(rng.integers(9, 19)))).isoformat(),
                             "event_type": "設計書作成", "rep_id": None,
                             "payload": {"product_id": products.product_id[rng.choice(12, p=prod_p)], "channel": rng.choice(["対面", "オンライン"])}})
        shock_now = any(s in (t, t + 1) for s in shock[i])
        if shock_now and any(s == t for s in shock[i]):
            eid += 1
            act_rows.append({"event_id": f"E{eid:08d}", "agency_id": aid, "event_ts": f"{m.year}-{m.month:02d}-15T10:00:00",
                             "event_type": "募集人退職", "rep_id": None, "payload": {"count": int(max(1, staff[i, t - 1] - staff[i, t])) if t else 1}})
        for _ in range(visits[i, t]):
            eid += 1; nid += 1
            d = pd.Timestamp(year=m.year, month=m.month, day=int(rng.integers(1, m.days_in_month + 1)))
            act_rows.append({"event_id": f"E{eid:08d}", "agency_id": aid, "event_ts": (d + pd.Timedelta(hours=14)).isoformat(),
                             "event_type": "訪問", "rep_id": rid, "payload": {"minutes": int(rng.choice([30, 45, 60, 90]))}})
            parts = [rng.choice(topics_ok)]
            if shock_now:
                parts.insert(0, rng.choice(topics_risk).format(n=rng.choice(last) if rng.random() < .5 else int(rng.integers(2, 4))))
            if lapse_q[i] > 0.09 and rng.random() < 0.5:
                parts.append(rng.choice(topics_lapse))
            note_rows.append({"note_id": f"N{nid:07d}", "agency_id": aid, "rep_id": rid, "visit_date": d.strftime("%Y-%m-%d"),
                              "note_text": "。".join(parts) + "。"})
        if rng.random() < 0.15:
            eid += 1
            act_rows.append({"event_id": f"E{eid:08d}", "agency_id": aid, "event_ts": f"{m.year}-{m.month:02d}-20T13:00:00",
                             "event_type": "研修", "rep_id": rid, "payload": {"theme": rng.choice(["コンプライアンス", "商品勉強会", "提案力強化"])}})
        if rng.random() < 0.1:
            eid += 1
            act_rows.append({"event_id": f"E{eid:08d}", "agency_id": aid, "event_ts": f"{m.year}-{m.month:02d}-05T10:00:00",
                             "event_type": "募集人登録", "rep_id": None, "payload": {"count": 1}})
print(len(act_rows), "activity events /", len(note_rows), "visit notes")

# ---------- 目標(年度は4月始まり) ----------
mon_prem = policies.assign(ym=policies.contract_date.str[:7]).groupby(["agency_id", "ym"]).annualized_premium_yen.sum()
avg_prem = mon_prem.groupby("agency_id").mean()
tg = []
for fy in [2024, 2025, 2026]:
    for k in range(12):
        p = pd.Period(f"{fy}-04", "M") + k
        for a in agencies.agency_id:
            tg.append((a, f"{p.year}-{p.month:02d}", fy, int(round(avg_prem.get(a, 1e6) * rng.uniform(1.0, 1.15) * (1 + 0.04 * (fy - 2024)), -4))))
targets = pd.DataFrame(tg, columns=["agency_id", "target_month", "fiscal_year", "target_premium_yen"])

# COMMAND ----------

# ---------- 書き出し(月次ファイルに分割) ----------
for sub in ["master", "policies", "lapses", "activities", "visit_notes", "targets"]:
    p = f"{OUT}/{sub}"
    if os.path.exists(p):
        shutil.rmtree(p)
    os.makedirs(p, exist_ok=True)

agencies.to_csv(f"{OUT}/master/agencies.csv", index=False)
reps.to_csv(f"{OUT}/master/sales_reps.csv", index=False)
products.to_csv(f"{OUT}/master/products.csv", index=False)
for fy, g in targets.groupby("fiscal_year"):
    g.to_csv(f"{OUT}/targets/targets_FY{fy}.csv", index=False)

pol_ym = policies.contract_date.str[:7].str.replace("-", "")
for ym, g in policies.groupby(pol_ym):
    g.to_csv(f"{OUT}/policies/policies_{ym}.csv", index=False)
lap_ym = lapses.event_date.str[:7].str.replace("-", "")
for ym, g in lapses.groupby(lap_ym):
    g.to_csv(f"{OUT}/lapses/lapses_{ym}.csv", index=False)

acts = pd.DataFrame(act_rows)
for ym, g in acts.groupby(acts.event_ts.str[:7].str.replace("-", "")):
    with open(f"{OUT}/activities/activities_{ym}.jsonl", "w") as f:
        for r in g.to_dict("records"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
notes = pd.DataFrame(note_rows)
for ym, g in notes.groupby(notes.visit_date.str[:7].str.replace("-", "")):
    with open(f"{OUT}/visit_notes/visit_notes_{ym}.jsonl", "w") as f:
        for r in g.to_dict("records"):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

summary = {sub: len(os.listdir(f"{OUT}/{sub}")) for sub in ["master", "policies", "lapses", "activities", "visit_notes", "targets"]}
print("files per folder:", summary)
print("rows:", {"agencies": len(agencies), "reps": len(reps), "products": len(products), "policies": len(policies),
                "lapses": len(lapses), "activities": len(acts), "visit_notes": len(notes), "targets": len(targets)})
print("recent-shock agencies (正解ラベルの確認用):", sorted(agencies.agency_id[recent].tolist()))

# COMMAND ----------

display(agencies.drop(columns=["contact_phone", "contact_email"]).head(10))
display(notes.tail(5))
