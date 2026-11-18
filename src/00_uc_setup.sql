-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00. Unity Catalog セットアップ
-- MAGIC スキーマ・Volume・**PII マスク関数**を最初に作る。マスク関数は Lakeflow パイプラインの Silver テーブル定義(`MASK`句)から参照するので、パイプラインより前に必要。

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema) COMMENT '代理店営業の訪問準備デモ(架空の生命保険会社「ひなた生命」、合成データ)';
USE SCHEMA IDENTIFIER(:schema);
CREATE VOLUME IF NOT EXISTS raw COMMENT '生データの着地点(CSV/JSON Lines)。Lakeflow Auto Loader で取り込む';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PII マスク関数
-- MAGIC 代理店の担当者名・電話・メールは、アカウントグループ `agency_pii_readers` のメンバーだけが平文で見られる。それ以外(営業担当・Genie・アプリのサービスプリンシパル)には `***` を返す。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask_pii(v STRING)
RETURNS STRING
COMMENT 'PII列のマスク。agency_pii_readers グループのメンバー以外には *** を返す'
RETURN CASE WHEN is_account_group_member('agency_pii_readers') THEN v ELSE '***' END;

-- COMMAND ----------

SELECT current_user() AS who, mask_pii('03-1234-5678') AS masked_for_me;

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED mask_pii;
