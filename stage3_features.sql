-- MySQL 8+
-- Stage 3: Feature engineering + customer intelligence

DROP TABLE IF EXISTS mart_customer_features;

CREATE TABLE mart_customer_features AS
WITH
ordered_amount AS (
  SELECT amount, NTILE(100) OVER (ORDER BY amount) AS pctl
  FROM fact_transaction
),
hv_threshold AS (
  SELECT MIN(amount) AS hv_amount
  FROM ordered_amount
  WHERE pctl = 95
),
analysis_date AS (
  SELECT DATE_ADD('2020-01-01', INTERVAL MAX(step) DAY) AS as_of_date
  FROM fact_transaction
),
activity AS (
  SELECT
    ao.customer_id,
    ft.source_hash,
    DATE_ADD('2020-01-01', INTERVAL ft.step DAY) AS txn_date,
    ft.amount,
    ft.isFraud,
    'OUT' AS direction,
    ad.account_type AS counterparty_type
  FROM fact_transaction ft
  JOIN dim_account ao ON ao.account_id = ft.origin_account_id
  JOIN dim_account ad ON ad.account_id = ft.destination_account_id

  UNION ALL

  SELECT
    ad.customer_id,
    ft.source_hash,
    DATE_ADD('2020-01-01', INTERVAL ft.step DAY) AS txn_date,
    ft.amount,
    ft.isFraud,
    'IN' AS direction,
    ao.account_type AS counterparty_type
  FROM fact_transaction ft
  JOIN dim_account ao ON ao.account_id = ft.origin_account_id
  JOIN dim_account ad ON ad.account_id = ft.destination_account_id
),
agg AS (
  SELECT
    a.customer_id,
    MAX(a.txn_date) AS last_txn_date,
    COUNT(*) AS frequency,
    SUM(a.amount) AS monetary,
    AVG(a.amount) AS avg_txn_amount,
    AVG(a.isFraud) AS fraud_rate,
    AVG(CASE WHEN a.amount >= h.hv_amount THEN 1 ELSE 0 END) AS high_value_ratio,
    AVG(CASE WHEN a.counterparty_type = 'MERCHANT' THEN 1 ELSE 0 END) AS merchant_exposure_ratio,
    SUM(CASE WHEN a.txn_date >= DATE_SUB(d.as_of_date, INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS txns_7d,
    SUM(CASE WHEN a.txn_date >= DATE_SUB(d.as_of_date, INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS txns_30d,
    SUM(CASE WHEN a.direction = 'IN' THEN a.amount ELSE 0 END) AS incoming_amount,
    SUM(CASE WHEN a.direction = 'OUT' THEN a.amount ELSE 0 END) AS outgoing_amount,
    d.as_of_date
  FROM activity a
  CROSS JOIN hv_threshold h
  CROSS JOIN analysis_date d
  GROUP BY a.customer_id, d.as_of_date
)
SELECT
  customer_id,
  DATEDIFF(as_of_date, last_txn_date) AS recency_days,
  frequency,
  ROUND(monetary, 2) AS monetary,
  ROUND(avg_txn_amount, 2) AS avg_txn_amount,
  ROUND(fraud_rate, 6) AS fraud_rate,
  ROUND(high_value_ratio, 6) AS high_value_ratio,
  ROUND(merchant_exposure_ratio, 6) AS merchant_exposure_ratio,
  txns_7d,
  txns_30d,
  ROUND(txns_7d / NULLIF(txns_30d, 0), 6) AS velocity_7d_30d_ratio,
  ROUND(incoming_amount - outgoing_amount, 2) AS net_flow,
  as_of_date
FROM agg;

ALTER TABLE mart_customer_features
  ADD PRIMARY KEY (customer_id);

DROP TABLE IF EXISTS mart_customer_segments;

CREATE TABLE mart_customer_segments AS
WITH scored AS (
  SELECT
    f.*,
    6 - NTILE(5) OVER (ORDER BY recency_days ASC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
  FROM mart_customer_features f
)
SELECT
  customer_id,
  recency_days,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  (r_score + f_score + m_score) AS rfm_score_total,
  fraud_rate,
  avg_txn_amount,
  high_value_ratio,
  merchant_exposure_ratio,
  txns_7d,
  txns_30d,
  velocity_7d_30d_ratio,
  net_flow,
  CASE
    WHEN fraud_rate >= 0.20 THEN 'HIGH_RISK'
    WHEN recency_days > 90 AND txns_30d = 0 THEN 'DORMANT'
    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 AND fraud_rate < 0.02 THEN 'PREMIER'
    WHEN merchant_exposure_ratio >= 0.70 AND frequency >= 10 THEN 'MERCHANT_HEAVY'
    WHEN velocity_7d_30d_ratio >= 0.60 AND high_value_ratio >= 0.20 THEN 'SURGING_HIGH_VALUE'
    WHEN (r_score + f_score + m_score) >= 12 THEN 'LOYAL_ACTIVE'
    WHEN (r_score + f_score + m_score) BETWEEN 9 AND 11 THEN 'GROWTH_POTENTIAL'
    WHEN (r_score + f_score + m_score) BETWEEN 6 AND 8 THEN 'MASS_MARKET'
    ELSE 'LOW_ENGAGEMENT'
  END AS segment_label,
  CASE
    WHEN fraud_rate >= 0.20 OR high_value_ratio >= 0.50 THEN 'HIGH'
    WHEN fraud_rate >= 0.05 OR high_value_ratio >= 0.20 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_band,
  as_of_date
FROM scored;

ALTER TABLE mart_customer_segments
  ADD PRIMARY KEY (customer_id);

SELECT COUNT(*) AS mart_customer_features_count FROM mart_customer_features;
SELECT COUNT(*) AS mart_customer_segments_count FROM mart_customer_segments;
