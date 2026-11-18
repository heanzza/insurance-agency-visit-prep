# Databricks notebook source
# MAGIC %md
# MAGIC # 03. ML: 挙績減少リスクの予測(MLflow + Unity Catalog Model Registry)
# MAGIC
# MAGIC **問い**: 「来月から3か月で、挙績が直近3か月より20%以上落ちそうな代理店はどこか」
# MAGIC
# MAGIC 営業担当は1人で10社前後を担当し、全社を毎月深く見る時間はない。落ちそうな代理店を **落ちる前に** 見つけて、先に訪問する。
# MAGIC
# MAGIC - 学習データ: `gold_agency_monthly_kpi` から、各月時点のスナップショット(代理店 × 月)を作る
# MAGIC - 特徴量: 挙績・設計書作成数・訪問回数・募集人の退職・解約・目標達成率 など(すべて「その月までに分かる」情報だけ)
# MAGIC - ラベル: 次の3か月の挙績 < 直近3か月の挙績 × 0.8
# MAGIC - 検証: 時系列で分割(2025年12月までで学習、2026年1〜7月で検証)。未来の情報が混ざらないようにする
# MAGIC - 出力: UC にモデルを登録(alias `champion`)→ 最新月を一括スコアリング → `ml_agency_risk_scores`(05 で訪問優先度リストにまとめ、Lakebase に同期する)

# COMMAND ----------

# MAGIC %pip install -q "mlflow>=3.1" scikit-learn
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "jp_fsi_catalog")
dbutils.widgets.text("schema", "agency_visit_prep")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
FQ = f"{C}.{S}"
MODEL_NAME = f"{FQ}.agency_decline_risk"

import json
import numpy as np
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score, average_precision_score

mlflow.set_registry_uri("databricks-uc")
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/agency_visit_prep_decline_risk")

kpi = spark.table(f"{FQ}.gold_agency_monthly_kpi").toPandas()
agencies = spark.table(f"{FQ}.gold_agency_360").select("agency_id", "channel_type").toPandas()
kpi["month"] = pd.to_datetime(kpi["month"])
kpi = kpi.sort_values(["agency_id", "month"])
print(kpi.shape, kpi.month.min().date(), "→", kpi.month.max().date())

# COMMAND ----------

# MAGIC %md ### 特徴量とラベル

# COMMAND ----------

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("agency_id", group_keys=False)
    r = lambda col, w: g[col].transform(lambda s: s.rolling(w, min_periods=w).sum())
    out = df[["agency_id", "month"]].copy()
    prem_l3 = r("premium_yen", 3)
    prem_l6 = r("premium_yen", 6)
    prop_l3 = r("proposals", 3)
    prop_l6 = r("proposals", 6)
    out["premium_last3m"] = prem_l3
    out["premium_change_3m"] = prem_l3 / (prem_l6 - prem_l3).replace(0, np.nan) - 1
    out["proposals_last3m"] = prop_l3
    out["proposals_change_3m"] = prop_l3 / (prop_l6 - prop_l3).replace(0, np.nan) - 1
    out["proposals_last1m_vs_avg"] = df["proposals"] / (prop_l6 / 6).replace(0, np.nan) - 1
    out["visits_last3m"] = r("visits", 3)
    out["staff_departures_6m"] = r("staff_departures", 6)
    out["staff_joins_6m"] = r("staff_joins", 6)
    out["lapse_ratio_6m"] = r("lapses", 6) / r("new_policies", 6).replace(0, np.nan)
    out["achievement_last3m"] = prem_l3 / r("target_premium_yen", 3).replace(0, np.nan)
    out["corp_share_6m"] = r("corp_premium_yen", 6) / prem_l6.replace(0, np.nan)
    out["conversion_3m"] = r("new_policies", 3) / prop_l3.replace(0, np.nan)
    # ラベル: 次の3か月の挙績
    out["premium_next3m"] = g["premium_yen"].transform(lambda s: s.shift(-1) + s.shift(-2) + s.shift(-3))
    out = out.merge(agencies, on="agency_id", how="left")
    out["channel_code"] = out["channel_type"].map({"乗合代理店": 0, "専属代理店": 1, "来店型ショップ": 2, "金融機関代理店": 3})
    return out

FEATURES = ["premium_last3m", "premium_change_3m", "proposals_last3m", "proposals_change_3m", "proposals_last1m_vs_avg",
            "visits_last3m", "staff_departures_6m", "staff_joins_6m", "lapse_ratio_6m", "achievement_last3m",
            "corp_share_6m", "conversion_3m", "channel_code"]
FEATURE_JA = {"premium_last3m": "直近3か月の挙績", "premium_change_3m": "挙績の変化(前3か月比)",
              "proposals_last3m": "直近3か月の設計書作成数", "proposals_change_3m": "設計書作成数の変化(前3か月比)",
              "proposals_last1m_vs_avg": "先月の設計書作成数(6か月平均比)", "visits_last3m": "直近3か月の訪問回数",
              "staff_departures_6m": "直近6か月の募集人退職", "staff_joins_6m": "直近6か月の募集人登録",
              "lapse_ratio_6m": "解約・失効の比率", "achievement_last3m": "直近3か月の目標達成率",
              "corp_share_6m": "法人比率", "conversion_3m": "設計書→契約の転換率", "channel_code": "代理店の形態"}

feat = build_features(kpi)
labeled = feat.dropna(subset=["premium_next3m", "premium_last3m", "premium_change_3m"]).copy()
labeled = labeled[labeled.premium_last3m > 0]
labeled["label"] = (labeled.premium_next3m < labeled.premium_last3m * 0.8).astype(int)

train = labeled[labeled.month <= "2025-12-01"]
valid = labeled[(labeled.month >= "2026-01-01") & (labeled.month <= "2026-07-01")]
print(f"train={len(train)} rows (positive {train.label.mean():.1%}) / valid={len(valid)} rows (positive {valid.label.mean():.1%})")

# COMMAND ----------

# MAGIC %md ### 学習と評価(MLflow に記録)
# MAGIC 業務の指標で評価する: **「リスク上位20社を訪問したら、実際に挙績が落ちた代理店の何割を先回りできたか」**(月ごとに計算して平均)

# COMMAND ----------

def top_k_capture(df, prob, k=20):
    d = df.assign(prob=prob)
    rows = []
    for m, g in d.groupby("month"):
        top = g.nlargest(k, "prob")
        rows.append({"month": m, "precision_at_k": top.label.mean(),
                     "recall_at_k": top.label.sum() / max(g.label.sum(), 1),
                     "baseline_precision": g.label.mean()})
    return pd.DataFrame(rows)

params = {"max_iter": 300, "learning_rate": 0.05, "max_depth": 4, "min_samples_leaf": 40, "l2_regularization": 1.0, "random_state": 7}
with mlflow.start_run(run_name="hgb_decline_risk") as run:
    model = HistGradientBoostingClassifier(**params)
    model.fit(train[FEATURES], train.label)
    p_valid = model.predict_proba(valid[FEATURES])[:, 1]
    auc = roc_auc_score(valid.label, p_valid)
    ap = average_precision_score(valid.label, p_valid)
    cap = top_k_capture(valid, p_valid, 20)
    metrics = {"valid_auc": auc, "valid_avg_precision": ap,
               "valid_precision_at_20": cap.precision_at_k.mean(), "valid_recall_at_20": cap.recall_at_k.mean(),
               "valid_base_rate": cap.baseline_precision.mean(), "train_rows": len(train), "valid_rows": len(valid)}
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)

    imp = permutation_importance(model, valid[FEATURES], valid.label, scoring="roc_auc", n_repeats=5, random_state=0)
    importance = pd.DataFrame({"feature": FEATURES, "feature_ja": [FEATURE_JA[f] for f in FEATURES],
                               "importance": imp.importances_mean}).sort_values("importance", ascending=False)
    mlflow.log_table(importance, "permutation_importance.json")
    mlflow.log_table(cap.assign(month=cap.month.astype(str)), "top20_capture_by_month.json")

    sig = infer_signature(valid[FEATURES], p_valid)
    mlflow.sklearn.log_model(model, name="model", signature=sig, input_example=valid[FEATURES].head(3),
                             registered_model_name=MODEL_NAME, pyfunc_predict_fn="predict_proba")
    run_id = run.info.run_id

print(json.dumps({k: round(float(v), 4) for k, v in metrics.items()}, indent=2))
display(importance)
display(cap)

# COMMAND ----------

from mlflow import MlflowClient
client = MlflowClient()
latest = max(client.search_model_versions(f"name='{MODEL_NAME}'"), key=lambda v: int(v.version))
client.set_registered_model_alias(MODEL_NAME, "champion", latest.version)
client.update_registered_model(MODEL_NAME, description="代理店の挙績減少リスク(次の3か月で直近3か月比 -20% 以上)を予測する。入力は gold_agency_monthly_kpi から作る13特徴量")
print(f"registered {MODEL_NAME} v{latest.version} as @champion (run {run_id})")

# COMMAND ----------

# MAGIC %md ### 最新月で一括スコアリング → 訪問優先度リスト
# MAGIC - `risk_score`: 減少リスクの確率
# MAGIC - `premium_at_risk_yen`: リスク × 直近3か月の挙績。**守るべき挙績の大きさ**で並べる(小さい代理店ばかりが上位に来ないように)
# MAGIC - `risk_drivers`: なぜリスクが高いかの上位3つ(同じ月の全代理店と比べて、リスクを上げる向きに外れている特徴量 × 重要度)

# COMMAND ----------

champion = mlflow.pyfunc.load_model(f"models:/{MODEL_NAME}@champion")
asof = feat.month.max()
cur = feat[feat.month == asof].copy()
pred = champion.predict(cur[FEATURES])
cur["risk_score"] = np.asarray(pred)[:, 1] if np.asarray(pred).ndim == 2 else np.asarray(pred)

# リスクを上げる向き: +1 = 値が大きいほど危ない, -1 = 小さいほど危ない
direction = {"premium_change_3m": -1, "proposals_change_3m": -1, "proposals_last1m_vs_avg": -1, "visits_last3m": -1,
             "staff_departures_6m": 1, "lapse_ratio_6m": 1, "achievement_last3m": -1, "conversion_3m": -1}
w = importance.set_index("feature").importance.clip(lower=0)
def fmt(f, v):
    if f in ("premium_change_3m", "proposals_change_3m", "proposals_last1m_vs_avg"):
        return f"{FEATURE_JA[f]} {v:+.0%}"
    if f in ("lapse_ratio_6m", "achievement_last3m", "conversion_3m"):
        return f"{FEATURE_JA[f]} {v:.0%}"
    return f"{FEATURE_JA[f]} {v:.0f}"
drivers = []
for _, row in cur.iterrows():
    sc = []
    for f, d in direction.items():
        col = cur[f]
        z = (row[f] - col.mean()) / (col.std() or 1)
        if pd.notna(z) and d * z > 0.5:
            sc.append((d * z * (w.get(f, 0) + 1e-3), fmt(f, row[f])))
    if pd.notna(row["premium_change_3m"]) and row["premium_change_3m"] > 0.3:
        sc.append((w.get("premium_change_3m", 0) + 1e-3, f"直近3か月の挙績が前3か月比 {row['premium_change_3m']:+.0%}(大型契約の反動減に注意)"))
    if not sc:  # 目立った外れ値がない場合は、重要度の高い特徴量の値をそのまま示す
        for f in importance.feature.head(2):
            if f in direction and pd.notna(row[f]):
                sc.append((0, fmt(f, row[f])))
    drivers.append(json.dumps([s for _, s in sorted(sc, key=lambda x: -x[0])[:3]], ensure_ascii=False))
cur["risk_drivers"] = drivers
cur["premium_at_risk_yen"] = (cur.risk_score * cur.premium_last3m).round().astype("int64")
cur["risk_level"] = pd.cut(cur.risk_score, [-1, 0.25, 0.5, 2], labels=["低", "中", "高"]).astype(str)
cur["model_version"] = int(latest.version)
cur["scored_month"] = asof.date()

scores = spark.createDataFrame(cur[["agency_id", "scored_month", "risk_score", "risk_level", "premium_at_risk_yen", "risk_drivers", "model_version"]])
scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.ml_agency_risk_scores")
spark.sql(f"COMMENT ON TABLE {FQ}.ml_agency_risk_scores IS 'ML の一括スコアリング結果(代理店ごとの挙績減少リスク)。モデル: {MODEL_NAME}@champion'")

# COMMAND ----------

top = spark.sql(f"""SELECT rank() OVER (ORDER BY s.premium_at_risk_yen DESC) AS rank, s.agency_id, a.agency_name, a.rep_name,
                           round(s.risk_score, 3) AS risk_score, s.risk_level, s.premium_at_risk_yen,
                           round(a.proposals_3m_change, 3) AS proposals_3m_change, a.staff_departures_6m, s.risk_drivers
                    FROM {FQ}.ml_agency_risk_scores s JOIN {FQ}.gold_agency_360 a USING (agency_id)
                    ORDER BY rank LIMIT 15""")
display(top)
print(top.toPandas().to_string())

# COMMAND ----------

# MAGIC %md ### 仕込んだ「直近で募集人が退職した22社」をどれだけ上位で拾えたか(合成データなので正解が分かる)

# COMMAND ----------

recent_shock = spark.sql(f"""
SELECT DISTINCT agency_id FROM {FQ}.silver_activities
WHERE event_type = '募集人退職' AND event_date >= '2026-07-01'""").toPandas().agency_id
pr = spark.table(f"{FQ}.ml_agency_risk_scores").select("agency_id", "risk_score", "premium_at_risk_yen").toPandas()
pr["priority_rank"] = pr.premium_at_risk_yen.rank(ascending=False, method="min")
hit = pr[pr.agency_id.isin(recent_shock)]
print(f"recent staff-departure agencies: {len(recent_shock)}")
print(f"  of which in top 30 by premium_at_risk: {(hit.priority_rank <= 30).sum()}")
print(f"  mean risk_score: shock={hit.risk_score.mean():.3f} vs others={pr[~pr.agency_id.isin(recent_shock)].risk_score.mean():.3f}")
dbutils.notebook.exit(json.dumps({"model": MODEL_NAME, "version": int(latest.version), **{k: round(float(v), 4) for k, v in metrics.items()}}))
