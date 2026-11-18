-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05. 訪問優先度リスト(Lakebase への同期元)
-- MAGIC Gold の代理店360 + ML のリスクスコア + GenAI のメモ要約 を1代理店1行にまとめる。
-- MAGIC この表を **Lakebase(Postgres)に同期** し、アプリはミリ秒で1代理店分を読む。

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

CREATE OR REPLACE VIEW v_visit_priority_src COMMENT 'gold_visit_priority の元(代理店360 + MLスコア + GenAI要約)' AS
SELECT
  a.agency_id, a.agency_name, a.channel_type, a.prefecture, a.established_year, a.rep_id, a.rep_name, a.rep_email, a.branch,
  a.data_as_of, a.fiscal_year, a.fytd_premium_yen, a.prior_fytd_premium_yen, a.fytd_yoy, a.fytd_target_yen, a.fytd_achievement_rate,
  a.premium_last3m_yen, a.premium_prev3m_yen, a.premium_3m_change, a.proposals_last3m, a.proposals_prev3m, a.proposals_3m_change,
  a.visits_last3m, a.last_visit_date, a.days_since_last_visit, a.staff_departures_6m, a.early_lapse_rate, a.pwaiver_rate_fytd, a.corp_share_fytd,
  s.risk_score, s.risk_level, s.premium_at_risk_yen, s.risk_drivers, s.model_version,
  n.note_summary, coalesce(n.staff_risk_notes_6m, 0) AS staff_risk_notes_6m, coalesce(n.competitor_notes_6m, 0) AS competitor_notes_6m,
  coalesce(n.lapse_complaint_notes_6m, 0) AS lapse_complaint_notes_6m, coalesce(n.opportunity_notes_6m, 0) AS opportunity_notes_6m,
  CAST(rank() OVER (ORDER BY s.premium_at_risk_yen DESC) AS INT) AS priority_rank,
  CAST(rank() OVER (PARTITION BY a.rep_id ORDER BY s.premium_at_risk_yen DESC) AS INT) AS rep_priority_rank
FROM gold_agency_360 a
JOIN ml_agency_risk_scores s USING (agency_id)
LEFT JOIN gold_agency_note_summary n USING (agency_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 同期先(Lakebase)は Change Data Feed を読んで差分だけ反映する。`CREATE OR REPLACE` で作り直すとスキーマが変わったとみなされ同期が止まるので、
-- MAGIC **表は1回だけ作り、以降は MERGE で更新する**(行の追加・更新・削除がそのまま CDF に乗る)。

-- COMMAND ----------

BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'gold_visit_priority') THEN
    CREATE TABLE gold_visit_priority
      COMMENT '訪問優先度リスト。代理店360 + 挙績減少リスク(ML) + 訪問メモ要約(GenAI)。Lakebase に同期してアプリが読む'
      TBLPROPERTIES (delta.enableChangeDataFeed = true)
      AS SELECT * FROM v_visit_priority_src WHERE false;
    ALTER TABLE gold_visit_priority ALTER COLUMN agency_id SET NOT NULL;
    ALTER TABLE gold_visit_priority ADD CONSTRAINT gold_visit_priority_pk PRIMARY KEY (agency_id);
    ALTER TABLE gold_visit_priority ALTER COLUMN risk_score COMMENT '挙績減少リスク(0〜1)。次の3か月で直近3か月比 -20%以上になる確率(MLモデル agency_decline_risk@champion)';
    ALTER TABLE gold_visit_priority ALTER COLUMN risk_level COMMENT 'リスク区分: 高(0.5以上) / 中(0.25以上) / 低';
    ALTER TABLE gold_visit_priority ALTER COLUMN premium_at_risk_yen COMMENT 'リスクにさらされた挙績 = risk_score × 直近3か月の挙績(円)。訪問優先度の並び順';
    ALTER TABLE gold_visit_priority ALTER COLUMN risk_drivers COMMENT 'リスクが高い理由の上位3つ(JSON配列、日本語)';
    ALTER TABLE gold_visit_priority ALTER COLUMN model_version COMMENT 'スコアに使ったモデルのバージョン';
    ALTER TABLE gold_visit_priority ALTER COLUMN note_summary COMMENT '直近90日の訪問メモの要点(GenAI 要約)';
    ALTER TABLE gold_visit_priority ALTER COLUMN priority_rank COMMENT '全社での訪問優先順位(premium_at_risk_yen の大きい順、1が最優先)';
    ALTER TABLE gold_visit_priority ALTER COLUMN rep_priority_rank COMMENT '担当営業の中での訪問優先順位';
    ALTER TABLE gold_visit_priority SET TAGS ('domain' = 'sales', 'layer' = 'gold', 'data_origin' = 'synthetic');
  END IF;
END;

-- COMMAND ----------

MERGE INTO gold_visit_priority t
USING v_visit_priority_src s ON t.agency_id = s.agency_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- COMMAND ----------

SELECT priority_rank, agency_id, agency_name, rep_name, round(risk_score, 3) AS risk_score, risk_level,
       premium_at_risk_yen, round(proposals_3m_change, 3) AS proposals_3m_change, staff_departures_6m, staff_risk_notes_6m, risk_drivers
FROM gold_visit_priority ORDER BY priority_rank LIMIT 15;

-- COMMAND ----------

SELECT risk_level, count(*) AS agencies, sum(premium_at_risk_yen) AS premium_at_risk_yen, round(avg(risk_score), 3) AS avg_risk
FROM gold_visit_priority GROUP BY risk_level ORDER BY avg_risk DESC;
