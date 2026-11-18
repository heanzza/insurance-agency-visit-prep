-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04. GenAI: 訪問メモ(自由記述)を構造化する
-- MAGIC
-- MAGIC 営業担当の訪問メモは数字に表れない「兆し」を含む(主力募集人の退職予定、他社への乗り換え、解約の苦情…)。
-- MAGIC ところが自由記述のままでは集計も検索もできない。そこで AI Functions で2つの Gold テーブルを作る。
-- MAGIC
-- MAGIC | テーブル | 方法 | 用途 |
-- MAGIC |---|---|---|
-- MAGIC | `gold_visit_note_signals` | `ai_classify` で各メモをリスクシグナル5分類 | Genie で「退職の話が出た代理店は？」に答える |
-- MAGIC | `gold_agency_note_summary` | `ai_query`(Claude)で代理店ごとに直近90日のメモを要約 | アプリの訪問プラン生成の材料・Lakebase に同期 |

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_visit_note_signals
COMMENT '訪問メモ(直近6か月)を ai_classify でリスクシグナルに分類した結果'
AS
SELECT n.note_id, n.agency_id, n.rep_id, n.visit_date, n.note_text,
       ai_classify(n.note_text, ARRAY('募集人の退職・人手不足', '他社への流出', '早期解約・苦情', '前向きな商機', '特記事項なし')) AS signal_category
FROM silver_visit_notes n
WHERE n.visit_date > add_months((SELECT max(visit_date) FROM silver_visit_notes), -6);

ALTER TABLE gold_visit_note_signals ALTER COLUMN note_id COMMENT 'メモID';
ALTER TABLE gold_visit_note_signals ALTER COLUMN agency_id COMMENT '代理店ID';
ALTER TABLE gold_visit_note_signals ALTER COLUMN rep_id COMMENT '営業担当ID';
ALTER TABLE gold_visit_note_signals ALTER COLUMN visit_date COMMENT '訪問日';
ALTER TABLE gold_visit_note_signals ALTER COLUMN note_text COMMENT '訪問メモの本文';
ALTER TABLE gold_visit_note_signals ALTER COLUMN signal_category COMMENT 'AI が分類したシグナル: 募集人の退職・人手不足 / 他社への流出 / 早期解約・苦情 / 前向きな商機 / 特記事項なし';
ALTER TABLE gold_visit_note_signals SET TAGS ('domain' = 'sales', 'layer' = 'gold', 'data_origin' = 'synthetic');

-- COMMAND ----------

SELECT signal_category, count(*) AS notes, count(DISTINCT agency_id) AS agencies
FROM gold_visit_note_signals GROUP BY signal_category ORDER BY notes DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_agency_note_summary
COMMENT '代理店ごとに直近90日の訪問メモを ai_query(Claude)で要約したもの'
AS
WITH recent AS (
  SELECT agency_id,
         count(*) AS notes_90d,
         array_join(transform(array_sort(collect_list(struct(visit_date, note_text))), x -> concat(x.visit_date, ': ', x.note_text)), '\n') AS notes
  FROM silver_visit_notes
  WHERE visit_date > (SELECT max(visit_date) FROM silver_visit_notes) - INTERVAL 90 DAYS
  GROUP BY agency_id
),
sig AS (
  SELECT agency_id,
         count_if(signal_category = '募集人の退職・人手不足') AS staff_risk_notes_6m,
         count_if(signal_category = '他社への流出') AS competitor_notes_6m,
         count_if(signal_category = '早期解約・苦情') AS lapse_complaint_notes_6m,
         count_if(signal_category = '前向きな商機') AS opportunity_notes_6m
  FROM gold_visit_note_signals GROUP BY agency_id
)
SELECT r.agency_id, r.notes_90d,
       ai_query('databricks-claude-sonnet-4-5',
         concat('あなたは生命保険会社の代理店営業のマネージャーです。次はある代理店への直近90日の訪問メモです。',
                '次回の訪問前に担当者が読む「要点」を、日本語2〜3文・150字以内で書いてください。',
                'リスク(退職・他社流出・解約)があれば必ず最初に書く。前置きや見出しは不要。\n\n', r.notes)) AS note_summary,
       coalesce(s.staff_risk_notes_6m, 0) AS staff_risk_notes_6m,
       coalesce(s.competitor_notes_6m, 0) AS competitor_notes_6m,
       coalesce(s.lapse_complaint_notes_6m, 0) AS lapse_complaint_notes_6m,
       coalesce(s.opportunity_notes_6m, 0) AS opportunity_notes_6m
FROM recent r LEFT JOIN sig s USING (agency_id);

ALTER TABLE gold_agency_note_summary ALTER COLUMN agency_id COMMENT '代理店ID';
ALTER TABLE gold_agency_note_summary ALTER COLUMN notes_90d COMMENT '直近90日の訪問メモの件数';
ALTER TABLE gold_agency_note_summary ALTER COLUMN note_summary COMMENT '直近90日の訪問メモの要点(Claude による要約、150字以内)';
ALTER TABLE gold_agency_note_summary ALTER COLUMN staff_risk_notes_6m COMMENT '直近6か月で「募集人の退職・人手不足」に分類されたメモの件数';
ALTER TABLE gold_agency_note_summary ALTER COLUMN competitor_notes_6m COMMENT '直近6か月で「他社への流出」に分類されたメモの件数';
ALTER TABLE gold_agency_note_summary ALTER COLUMN lapse_complaint_notes_6m COMMENT '直近6か月で「早期解約・苦情」に分類されたメモの件数';
ALTER TABLE gold_agency_note_summary ALTER COLUMN opportunity_notes_6m COMMENT '直近6か月で「前向きな商機」に分類されたメモの件数';
ALTER TABLE gold_agency_note_summary SET TAGS ('domain' = 'sales', 'layer' = 'gold', 'data_origin' = 'synthetic');

-- COMMAND ----------

SELECT s.agency_id, a.agency_name, s.notes_90d, s.staff_risk_notes_6m, s.competitor_notes_6m, s.note_summary
FROM gold_agency_note_summary s JOIN gold_agency_360 a USING (agency_id)
ORDER BY s.staff_risk_notes_6m DESC, s.competitor_notes_6m DESC
LIMIT 10;
