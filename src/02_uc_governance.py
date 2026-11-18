# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Unity Catalog ガバナンス
# MAGIC Lakeflow パイプラインが作ったテーブルに対して、次の4つを確認・設定する。
# MAGIC 1. **タグ**: レイヤー(bronze/silver/gold)、業務ドメイン、合成データであること、PII 列の分類
# MAGIC 2. **列マスク**: 代理店の連絡先(PII)は `mask_pii` で `***` になる(パイプライン定義の `MASK` 句で設定済み)
# MAGIC 3. **メタデータ**: Gold の全列に日本語コメントがあるか(Genie の精度に直結する)
# MAGIC 4. **リネージ**: Volume の生ファイル → Bronze → Silver → Gold のつながりを system テーブルで確認する

# COMMAND ----------

dbutils.widgets.text("catalog", "jp_fsi_catalog")
dbutils.widgets.text("schema", "agency_visit_prep")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
FQ = f"{C}.{S}"

tables = spark.sql(f"""SELECT table_name, table_type FROM {C}.information_schema.tables
                       WHERE table_schema = '{S}' ORDER BY table_name""").collect()
display(spark.createDataFrame(tables))

# COMMAND ----------

# MAGIC %md ### 1. タグ
# MAGIC このワークスペースには **管理タグ(タグポリシー)** があり、`domain` は決められた値(`sales` など)しか付けられない。ルールに従って付ける。

# COMMAND ----------

def alter_kw(table_type):
    return {"MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE"}.get(table_type, "TABLE")

for t in tables:
    name, ttype = t.table_name, t.table_type
    layer = name.split("_")[0]
    if layer not in ("bronze", "silver", "gold"):
        continue
    spark.sql(f"ALTER {alter_kw(ttype)} {FQ}.{name} SET TAGS ('domain' = 'sales', 'layer' = '{layer}', 'data_origin' = 'synthetic')")

pii_cols = {"contact_person": "name", "contact_phone": "phone", "contact_email": "email"}
for col, cat in pii_cols.items():
    spark.sql(f"ALTER MATERIALIZED VIEW {FQ}.silver_agencies ALTER COLUMN {col} SET TAGS ('sensitivity' = 'pii', 'pii_category' = '{cat}')")
    # Bronze は生データなので同じ列にタグだけ付ける(Bronze は利用者に GRANT しない運用)
    spark.sql(f"ALTER STREAMING TABLE {FQ}.bronze_agencies ALTER COLUMN {col} SET TAGS ('sensitivity' = 'pii', 'pii_category' = '{cat}')")
spark.sql(f"ALTER STREAMING TABLE {FQ}.silver_visit_notes ALTER COLUMN note_text SET TAGS ('sensitivity' = 'internal')")

display(spark.sql(f"""SELECT table_name, tag_name, tag_value FROM {C}.information_schema.table_tags
                      WHERE schema_name = '{S}' ORDER BY table_name, tag_name"""))
display(spark.sql(f"""SELECT table_name, column_name, tag_name, tag_value FROM {C}.information_schema.column_tags
                      WHERE schema_name = '{S}' ORDER BY table_name, column_name, tag_name"""))

# COMMAND ----------

# MAGIC %md ### 2. 列マスクの確認
# MAGIC 実行ユーザーは `agency_pii_readers` に入っていないので、連絡先は `***` で返るはず。

# COMMAND ----------

display(spark.sql(f"""SELECT table_name, column_name, mask_name FROM {C}.information_schema.column_masks
                      WHERE table_schema = '{S}'"""))
masked = spark.sql(f"SELECT agency_id, agency_name, contact_person, contact_phone, contact_email FROM {FQ}.silver_agencies ORDER BY agency_id LIMIT 5")
display(masked)
assert all(r.contact_phone == "***" for r in masked.collect()), "マスクが効いていない"
print("OK: PII columns are masked for", spark.sql("SELECT current_user()").first()[0])

# COMMAND ----------

# MAGIC %md ### 3. メタデータ(列コメント)の充足率

# COMMAND ----------

cov = spark.sql(f"""
SELECT table_name, count(*) AS columns, count(comment) AS commented,
       round(count(comment) / count(*) * 100, 1) AS coverage_pct
FROM {C}.information_schema.columns
WHERE table_schema = '{S}' AND (table_name LIKE 'silver_%' OR table_name LIKE 'gold_%')
GROUP BY table_name ORDER BY table_name""")
display(cov)
gold_gap = [r.table_name for r in cov.collect() if r.table_name.startswith("gold_") and r.coverage_pct < 100]
print("gold tables with missing comments:", gold_gap or "none")

# COMMAND ----------

# MAGIC %md ### 4. リネージ
# MAGIC `system.access.table_lineage` は書き込み後に数分遅れて反映される。初回実行直後は空のことがある。

# COMMAND ----------

lin = spark.sql(f"""
SELECT DISTINCT coalesce(source_table_full_name, source_path) AS source, target_table_full_name AS target
FROM system.access.table_lineage
WHERE target_table_full_name LIKE '{FQ}.%' AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY target, source""")
display(lin)
print("lineage edges:", lin.count())

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {FQ}"))
