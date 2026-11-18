-- Gold: 訪問準備に使う集計。基準日は current_date() ではなくデータの最終月(data_as_of)にして、デモの結果が実行日で変わらないようにする。

CREATE OR REFRESH MATERIALIZED VIEW gold_agency_monthly_kpi (
  agency_id STRING COMMENT '代理店ID',
  month DATE COMMENT '対象月(月初日)',
  fiscal_year INT COMMENT '年度(4月始まり。2026年4月〜2027年3月 = 2026年度)',
  new_policies BIGINT COMMENT '新契約件数',
  premium_yen BIGINT COMMENT '挙績 = 新契約の年換算保険料の合計(円)',
  corp_premium_yen BIGINT COMMENT 'うち法人契約の挙績(円)',
  pwaiver_eligible_policies BIGINT COMMENT 'P免を付けられる商品の新契約件数(P免付加率の分母)',
  pwaiver_attached_policies BIGINT COMMENT 'P免を付けた新契約件数(P免付加率の分子)',
  proposals BIGINT COMMENT '設計書作成数(契約の先行指標)',
  visits BIGINT COMMENT '営業担当の訪問回数',
  staff_departures BIGINT COMMENT '募集人の退職人数',
  staff_joins BIGINT COMMENT '募集人の登録人数',
  lapses BIGINT COMMENT 'その月に起きた解約・失効の件数',
  target_premium_yen BIGINT COMMENT '挙績目標(円)',
  achievement_rate DOUBLE COMMENT '目標達成率 = premium_yen / target_premium_yen'
)
COMMENT '代理店 × 月の KPI。Genie と ML の特徴量の土台'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH months AS (
  SELECT explode(sequence(date_trunc('MONTH', min(contract_date))::date, date_trunc('MONTH', max(contract_date))::date, INTERVAL 1 MONTH)) AS month
  FROM silver_policies
),
spine AS (SELECT a.agency_id, m.month FROM silver_agencies a CROSS JOIN months m),
pol AS (
  SELECT p.agency_id, date_trunc('MONTH', p.contract_date)::date AS month,
         count(*) AS new_policies, sum(p.annualized_premium_yen) AS premium_yen,
         sum(CASE WHEN p.customer_type = '法人' THEN p.annualized_premium_yen ELSE 0 END) AS corp_premium_yen,
         count_if(pr.p_waiver_available) AS pwaiver_eligible_policies,
         count_if(pr.p_waiver_available AND p.has_p_waiver) AS pwaiver_attached_policies
  FROM silver_policies p JOIN silver_products pr USING (product_id)
  GROUP BY ALL
),
act AS (
  SELECT agency_id, date_trunc('MONTH', event_date)::date AS month,
         count_if(event_type = '設計書作成') AS proposals,
         count_if(event_type = '訪問') AS visits,
         coalesce(sum(CASE WHEN event_type = '募集人退職' THEN staff_count_change END), 0) AS staff_departures,
         coalesce(sum(CASE WHEN event_type = '募集人登録' THEN staff_count_change END), 0) AS staff_joins
  FROM silver_activities GROUP BY ALL
),
lap AS (
  SELECT p.agency_id, date_trunc('MONTH', l.event_date)::date AS month, count(*) AS lapses
  FROM silver_lapses l JOIN silver_policies p USING (policy_id) GROUP BY ALL
)
SELECT s.agency_id, s.month,
       CASE WHEN month(s.month) >= 4 THEN year(s.month) ELSE year(s.month) - 1 END AS fiscal_year,
       coalesce(pol.new_policies, 0) AS new_policies,
       coalesce(pol.premium_yen, 0) AS premium_yen,
       coalesce(pol.corp_premium_yen, 0) AS corp_premium_yen,
       coalesce(pol.pwaiver_eligible_policies, 0) AS pwaiver_eligible_policies,
       coalesce(pol.pwaiver_attached_policies, 0) AS pwaiver_attached_policies,
       coalesce(act.proposals, 0) AS proposals,
       coalesce(act.visits, 0) AS visits,
       coalesce(act.staff_departures, 0) AS staff_departures,
       coalesce(act.staff_joins, 0) AS staff_joins,
       coalesce(lap.lapses, 0) AS lapses,
       t.target_premium_yen,
       try_divide(coalesce(pol.premium_yen, 0), t.target_premium_yen) AS achievement_rate
FROM spine s
LEFT JOIN pol USING (agency_id, month)
LEFT JOIN act USING (agency_id, month)
LEFT JOIN lap USING (agency_id, month)
LEFT JOIN silver_targets t USING (agency_id, month);


CREATE OR REFRESH MATERIALIZED VIEW gold_agency_360 (
  agency_id STRING NOT NULL COMMENT '代理店ID',
  agency_name STRING COMMENT '代理店名',
  channel_type STRING COMMENT '代理店の形態',
  prefecture STRING COMMENT '都道府県',
  established_year INT COMMENT '設立年',
  rep_id STRING COMMENT '担当営業ID',
  rep_name STRING COMMENT '担当営業の氏名',
  rep_email STRING COMMENT '担当営業のメール',
  branch STRING COMMENT '担当営業の所属営業部',
  data_as_of DATE COMMENT 'データの基準日(最終月の月末)。訪問準備の「今日」',
  fiscal_year INT COMMENT '基準日の年度',
  fytd_premium_yen BIGINT COMMENT '今年度累計の挙績(4月〜基準月、円)',
  prior_fytd_premium_yen BIGINT COMMENT '前年度の同じ期間の挙績(円)',
  fytd_yoy DOUBLE COMMENT '前年比 = 今年度累計 / 前年度同期間 - 1',
  fytd_target_yen BIGINT COMMENT '今年度累計の目標(円)',
  fytd_achievement_rate DOUBLE COMMENT '今年度累計の目標達成率',
  premium_last3m_yen BIGINT COMMENT '直近3か月の挙績(円)',
  premium_prev3m_yen BIGINT COMMENT 'その前の3か月の挙績(円)',
  premium_3m_change DOUBLE COMMENT '直近3か月の挙績の変化率(前の3か月比)',
  proposals_last3m BIGINT COMMENT '直近3か月の設計書作成数',
  proposals_prev3m BIGINT COMMENT 'その前の3か月の設計書作成数',
  proposals_3m_change DOUBLE COMMENT '設計書作成数の変化率(前の3か月比)。契約の先行指標',
  visits_last3m BIGINT COMMENT '直近3か月の訪問回数',
  last_visit_date DATE COMMENT '最後に訪問した日',
  days_since_last_visit INT COMMENT '最後の訪問から基準日までの日数',
  staff_departures_6m BIGINT COMMENT '直近6か月の募集人退職人数',
  early_lapse_rate DOUBLE COMMENT '早期解約率 = 直近24か月の新契約のうち、契約から13か月以内に解約・失効した割合',
  pwaiver_rate_fytd DOUBLE COMMENT 'P免付加率(今年度)。分母は P免を付けられる商品の契約だけ',
  corp_share_fytd DOUBLE COMMENT '法人比率(今年度、挙績ベース)'
)
COMMENT '代理店 360°ビュー。1代理店1行。訪問準備の画面・Genie・Lakebase 同期の元'
TBLPROPERTIES ('quality' = 'gold', 'delta.enableChangeDataFeed' = 'true')
AS
WITH asof AS (SELECT max(month) AS m FROM gold_agency_monthly_kpi WHERE new_policies > 0),
k AS (
  SELECT k.*, a.m AS asof_m,
         CASE WHEN month(a.m) >= 4 THEN year(a.m) ELSE year(a.m) - 1 END AS asof_fy,
         months_between(a.m, k.month) AS age_m
  FROM gold_agency_monthly_kpi k CROSS JOIN asof a
  WHERE k.month <= a.m
),
agg AS (
  SELECT agency_id, max(asof_m) AS asof_m, max(asof_fy) AS asof_fy,
    sum(CASE WHEN fiscal_year = asof_fy THEN premium_yen END) AS fytd_premium_yen,
    sum(CASE WHEN fiscal_year = asof_fy - 1 AND month <= add_months(asof_m, -12) THEN premium_yen END) AS prior_fytd_premium_yen,
    sum(CASE WHEN fiscal_year = asof_fy THEN target_premium_yen END) AS fytd_target_yen,
    sum(CASE WHEN age_m < 3 THEN premium_yen END) AS premium_last3m_yen,
    sum(CASE WHEN age_m BETWEEN 3 AND 5 THEN premium_yen END) AS premium_prev3m_yen,
    sum(CASE WHEN age_m < 3 THEN proposals END) AS proposals_last3m,
    sum(CASE WHEN age_m BETWEEN 3 AND 5 THEN proposals END) AS proposals_prev3m,
    sum(CASE WHEN age_m < 3 THEN visits END) AS visits_last3m,
    sum(CASE WHEN age_m < 6 THEN staff_departures END) AS staff_departures_6m,
    try_divide(sum(CASE WHEN fiscal_year = asof_fy THEN pwaiver_attached_policies END),
               sum(CASE WHEN fiscal_year = asof_fy THEN pwaiver_eligible_policies END)) AS pwaiver_rate_fytd,
    try_divide(sum(CASE WHEN fiscal_year = asof_fy THEN corp_premium_yen END),
               sum(CASE WHEN fiscal_year = asof_fy THEN premium_yen END)) AS corp_share_fytd
  FROM k GROUP BY agency_id
),
lv AS (SELECT agency_id, max(event_date) AS last_visit_date FROM silver_activities WHERE event_type = '訪問' GROUP BY agency_id),
el AS (
  SELECT p.agency_id,
         try_divide(count_if(l.policy_id IS NOT NULL AND months_between(l.event_date, p.contract_date) <= 13), count(*)) AS early_lapse_rate
  FROM silver_policies p
  LEFT JOIN silver_lapses l USING (policy_id)
  WHERE p.contract_date > add_months((SELECT last_day(m) FROM asof), -24)
  GROUP BY p.agency_id
)
SELECT a.agency_id, a.agency_name, a.channel_type, a.prefecture, a.established_year,
       a.rep_id, r.rep_name, r.email AS rep_email, r.branch,
       last_day(g.asof_m) AS data_as_of, g.asof_fy AS fiscal_year,
       g.fytd_premium_yen, g.prior_fytd_premium_yen,
       try_divide(g.fytd_premium_yen, g.prior_fytd_premium_yen) - 1 AS fytd_yoy,
       g.fytd_target_yen, try_divide(g.fytd_premium_yen, g.fytd_target_yen) AS fytd_achievement_rate,
       g.premium_last3m_yen, g.premium_prev3m_yen,
       try_divide(g.premium_last3m_yen, g.premium_prev3m_yen) - 1 AS premium_3m_change,
       g.proposals_last3m, g.proposals_prev3m,
       try_divide(g.proposals_last3m, g.proposals_prev3m) - 1 AS proposals_3m_change,
       g.visits_last3m, lv.last_visit_date,
       datediff(last_day(g.asof_m), lv.last_visit_date) AS days_since_last_visit,
       g.staff_departures_6m, el.early_lapse_rate, g.pwaiver_rate_fytd, g.corp_share_fytd
FROM silver_agencies a
JOIN agg g USING (agency_id)
LEFT JOIN silver_sales_reps r USING (rep_id)
LEFT JOIN lv USING (agency_id)
LEFT JOIN el USING (agency_id);
