-- Silver: 型をそろえ、データ品質ルール(Expectation)をかけ、PII をマスクする。

CREATE OR REFRESH MATERIALIZED VIEW silver_agencies (
  agency_id STRING NOT NULL COMMENT '代理店ID(A + 4桁)',
  agency_name STRING COMMENT '代理店名(架空)',
  channel_type STRING COMMENT '代理店の形態: 乗合代理店 / 専属代理店 / 来店型ショップ / 金融機関代理店',
  prefecture STRING COMMENT '所在地の都道府県',
  established_year INT COMMENT '設立年',
  rep_id STRING COMMENT '担当する営業担当のID(silver_sales_reps.rep_id)',
  contact_person STRING MASK ${catalog}.${schema}.mask_pii COMMENT '代理店側の窓口担当者名。PII(マスク対象)',
  contact_phone STRING MASK ${catalog}.${schema}.mask_pii COMMENT '代理店の電話番号。PII(マスク対象)',
  contact_email STRING MASK ${catalog}.${schema}.mask_pii COMMENT '代理店のメールアドレス。PII(マスク対象)',
  CONSTRAINT valid_agency_id EXPECT (agency_id RLIKE '^A[0-9]{4}$') ON VIOLATION DROP ROW
)
COMMENT '代理店マスタ。連絡先(PII)は mask_pii で列マスク'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT agency_id, agency_name, channel_type, prefecture, CAST(established_year AS INT) AS established_year, rep_id,
          contact_person, contact_phone, contact_email
FROM bronze_agencies;

CREATE OR REFRESH MATERIALIZED VIEW silver_sales_reps (
  rep_id STRING NOT NULL COMMENT '営業担当ID',
  rep_name STRING COMMENT '営業担当の氏名(架空)',
  branch STRING COMMENT '所属する営業部',
  email STRING COMMENT '営業担当の社内メール(アプリのログインユーザーとの対応に使う)'
)
COMMENT '保険会社側の営業担当(代理店営業)マスタ'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT rep_id, rep_name, branch, email FROM bronze_sales_reps;

CREATE OR REFRESH MATERIALIZED VIEW silver_products (
  product_id STRING NOT NULL COMMENT '商品ID',
  product_name STRING COMMENT '商品名',
  product_category STRING COMMENT '商品カテゴリ(終身保険・医療保険・変額保険 など)',
  avg_annual_premium_yen BIGINT COMMENT '平均的な年換算保険料(円)',
  p_waiver_available BOOLEAN COMMENT 'P免(保険料払込免除特約)を付けられる商品か。P免付加率の分母に使う'
)
COMMENT '商品マスタ'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT product_id, product_name, product_category, avg_annual_premium_yen, p_waiver_available FROM bronze_products;

CREATE OR REFRESH STREAMING TABLE silver_policies (
  policy_id STRING COMMENT '証券番号',
  agency_id STRING COMMENT '募集した代理店ID',
  product_id STRING COMMENT '商品ID',
  contract_date DATE COMMENT '契約日',
  annualized_premium_yen BIGINT COMMENT '年換算保険料(円)。挙績の金額',
  customer_type STRING COMMENT '契約者区分: 個人 / 法人',
  insured_age INT COMMENT '被保険者の年齢',
  has_p_waiver BOOLEAN COMMENT 'P免(保険料払込免除特約)を付けたか',
  CONSTRAINT agency_not_null EXPECT (agency_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_premium EXPECT (annualized_premium_yen > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (contract_date IS NOT NULL)
)
COMMENT '新契約(成立した保険契約)。挙績 = 年換算保険料(annualized_premium_yen)。不正行は Expectation で除外'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT policy_id, agency_id, product_id, contract_date, annualized_premium_yen, customer_type, insured_age,
          p_waiver_rider = 'あり' AS has_p_waiver
FROM STREAM(bronze_policies);

CREATE OR REFRESH STREAMING TABLE silver_lapses (
  policy_id STRING COMMENT '証券番号',
  event_date DATE COMMENT '解約・失効日',
  event_type STRING COMMENT '解約 / 失効',
  reason STRING COMMENT '解約・失効の理由',
  CONSTRAINT policy_not_null EXPECT (policy_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT '解約・失効のイベント'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT policy_id, event_date, event_type, reason FROM STREAM(bronze_lapses);

CREATE OR REFRESH STREAMING TABLE silver_activities (
  event_id STRING COMMENT 'イベントID',
  agency_id STRING COMMENT '代理店ID',
  event_ts TIMESTAMP COMMENT '発生日時',
  event_date DATE COMMENT '発生日',
  event_type STRING COMMENT '設計書作成 / 訪問 / 研修 / 募集人登録 / 募集人退職',
  rep_id STRING COMMENT '訪問・研修を行った営業担当ID',
  product_id STRING COMMENT '設計書の対象商品ID',
  visit_minutes INT COMMENT '訪問時間(分)',
  staff_count_change INT COMMENT '募集人の登録・退職人数',
  training_theme STRING COMMENT '研修テーマ',
  CONSTRAINT known_event_type EXPECT (event_type IN ('設計書作成', '訪問', '研修', '募集人登録', '募集人退職')) ON VIOLATION DROP ROW
)
COMMENT '代理店の活動ログ。設計書作成は契約の先行指標、訪問は営業担当の活動量'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT event_id, agency_id, event_ts, CAST(event_ts AS DATE) AS event_date, event_type, rep_id,
          payload.product_id AS product_id, payload.minutes AS visit_minutes,
          payload.count AS staff_count_change, payload.theme AS training_theme
FROM STREAM(bronze_activities);

CREATE OR REFRESH STREAMING TABLE silver_visit_notes (
  note_id STRING COMMENT 'メモID',
  agency_id STRING COMMENT '代理店ID',
  rep_id STRING COMMENT '営業担当ID',
  visit_date DATE COMMENT '訪問日',
  note_text STRING COMMENT '訪問メモの本文(自由記述)',
  CONSTRAINT note_not_empty EXPECT (length(note_text) > 0) ON VIOLATION DROP ROW
)
COMMENT '営業担当の訪問メモ(自由記述)。GenAI でリスクシグナルを抽出する元データ'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT note_id, agency_id, rep_id, visit_date, note_text FROM STREAM(bronze_visit_notes);

CREATE OR REFRESH MATERIALIZED VIEW silver_targets (
  agency_id STRING COMMENT '代理店ID',
  month DATE COMMENT '対象月(月初日)',
  fiscal_year INT COMMENT '年度(4月始まり)',
  target_premium_yen BIGINT COMMENT '挙績目標(年換算保険料、円)'
)
COMMENT '代理店別の月次挙績目標。年度は4月始まり'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT agency_id, to_date(target_month || '-01') AS month, fiscal_year, target_premium_yen
FROM bronze_targets;
