-- Bronze: Auto Loader (read_files) で Volume の生ファイルを増分取り込みする。
-- 列はそのまま。取り込み元ファイルと取り込み時刻を付ける。

CREATE OR REFRESH STREAMING TABLE bronze_agencies
COMMENT '代理店マスタ(生データ)。master/agencies.csv'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/master/', format => 'csv', header => true,
  pathGlobFilter => 'agencies.csv', schemaHints => 'established_year INT');

CREATE OR REFRESH STREAMING TABLE bronze_sales_reps
COMMENT '営業担当マスタ(生データ)。master/sales_reps.csv'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/master/', format => 'csv', header => true, pathGlobFilter => 'sales_reps.csv');

CREATE OR REFRESH STREAMING TABLE bronze_products
COMMENT '商品マスタ(生データ)。master/products.csv'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/master/', format => 'csv', header => true,
  pathGlobFilter => 'products.csv', schemaHints => 'avg_annual_premium_yen BIGINT, p_waiver_available BOOLEAN');

CREATE OR REFRESH STREAMING TABLE bronze_policies
COMMENT '新契約(生データ、月次CSV)。policies/policies_YYYYMM.csv'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/policies/', format => 'csv', header => true,
  schemaHints => 'contract_date DATE, annualized_premium_yen BIGINT, insured_age INT');

CREATE OR REFRESH STREAMING TABLE bronze_lapses
COMMENT '解約・失効(生データ、月次CSV)。lapses/lapses_YYYYMM.csv'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/lapses/', format => 'csv', header => true, schemaHints => 'event_date DATE');

CREATE OR REFRESH STREAMING TABLE bronze_activities
COMMENT '代理店の活動ログ(生データ、JSON Lines)。設計書作成・訪問・研修・募集人の登録/退職'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/activities/', format => 'json',
  schemaHints => 'event_ts TIMESTAMP, payload STRUCT<product_id STRING, channel STRING, minutes INT, count INT, theme STRING>');

CREATE OR REFRESH STREAMING TABLE bronze_visit_notes
COMMENT '営業担当の訪問メモ(生データ、JSON Lines、自由記述テキスト)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/visit_notes/', format => 'json', schemaHints => 'visit_date DATE');

CREATE OR REFRESH STREAMING TABLE bronze_targets
COMMENT '代理店別の月次挙績目標(生データ、年度別CSV)'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
FROM STREAM read_files('${raw_path}/targets/', format => 'csv', header => true,
  schemaHints => 'fiscal_year INT, target_premium_yen BIGINT');
