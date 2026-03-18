-- MySQL 8+
-- Stage 4: Analytics queries for BI/dashboard

-- 1) Top customers by value (Pareto)
WITH ranked AS (
  SELECT
    f.customer_id,
    f.monetary AS customer_value,
    ROW_NUMBER() OVER (ORDER BY f.monetary DESC) AS value_rank,
    COUNT(*) OVER () AS total_customers,
    SUM(f.monetary) OVER () AS total_value,
    SUM(f.monetary) OVER (
      ORDER BY f.monetary DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_value
  FROM mart_customer_features f
),
pareto AS (
  SELECT
    customer_id,
    customer_value,
    value_rank,
    total_customers,
    customer_value / NULLIF(total_value, 0) AS value_share_pct,
    cumulative_value / NULLIF(total_value, 0) AS cumulative_value_pct,
    value_rank / NULLIF(total_customers, 0) AS cumulative_customer_pct
  FROM ranked
)
SELECT
  customer_id,
  customer_value,
  value_rank,
  ROUND(value_share_pct, 6) AS value_share_pct,
  ROUND(cumulative_value_pct, 6) AS cumulative_value_pct,
  ROUND(cumulative_customer_pct, 6) AS cumulative_customer_pct,
  CASE WHEN cumulative_value_pct <= 0.80 THEN 'TOP_80_VALUE_CLUSTER' ELSE 'LONG_TAIL' END AS pareto_band
FROM pareto
ORDER BY value_rank;


-- 2) Monthly transaction trends
WITH monthly AS (
  SELECT
    DATE_FORMAT(DATE_ADD('2020-01-01', INTERVAL step DAY), '%Y-%m-01') AS month_start,
    COUNT(*) AS txn_count,
    SUM(amount) AS txn_value,
    AVG(amount) AS avg_ticket_size,
    AVG(isFraud) AS fraud_rate
  FROM fact_transaction
  GROUP BY DATE_FORMAT(DATE_ADD('2020-01-01', INTERVAL step DAY), '%Y-%m-01')
),
trend AS (
  SELECT
    month_start,
    txn_count,
    txn_value,
    avg_ticket_size,
    fraud_rate,
    LAG(txn_count) OVER (ORDER BY month_start) AS prev_txn_count,
    LAG(txn_value) OVER (ORDER BY month_start) AS prev_txn_value
  FROM monthly
)
SELECT
  month_start,
  txn_count,
  ROUND(txn_value, 2) AS txn_value,
  ROUND(avg_ticket_size, 2) AS avg_ticket_size,
  ROUND(fraud_rate, 6) AS fraud_rate,
  ROUND((txn_count - prev_txn_count) / NULLIF(prev_txn_count, 0), 6) AS mom_txn_count_growth,
  ROUND((txn_value - prev_txn_value) / NULLIF(prev_txn_value, 0), 6) AS mom_txn_value_growth
FROM trend
ORDER BY month_start;


-- 3) Segment-wise revenue contribution
WITH seg_value AS (
  SELECT
    s.segment_label,
    COUNT(*) AS customers,
    SUM(f.monetary) AS segment_value,
    AVG(f.monetary) AS avg_value_per_customer
  FROM mart_customer_segments s
  JOIN mart_customer_features f ON f.customer_id = s.customer_id
  GROUP BY s.segment_label
),
contrib AS (
  SELECT
    segment_label,
    customers,
    segment_value,
    avg_value_per_customer,
    SUM(segment_value) OVER () AS total_value,
    DENSE_RANK() OVER (ORDER BY segment_value DESC) AS value_rank
  FROM seg_value
)
SELECT
  segment_label,
  customers,
  ROUND(segment_value, 2) AS segment_value,
  ROUND(avg_value_per_customer, 2) AS avg_value_per_customer,
  ROUND(segment_value / NULLIF(total_value, 0), 6) AS value_contribution_pct,
  value_rank
FROM contrib
ORDER BY value_rank;


-- 4) Risk segment identification (watchlist)
WITH risk_base AS (
  SELECT
    s.customer_id,
    s.segment_label,
    s.risk_band,
    f.fraud_rate,
    f.high_value_ratio,
    f.merchant_exposure_ratio,
    COALESCE(f.velocity_7d_30d_ratio, 0) AS velocity_7d_30d_ratio,
    f.monetary,
    (
      0.50 * f.fraud_rate +
      0.20 * f.high_value_ratio +
      0.20 * COALESCE(f.velocity_7d_30d_ratio, 0) +
      0.10 * f.merchant_exposure_ratio
    ) AS composite_risk_score
  FROM mart_customer_segments s
  JOIN mart_customer_features f ON f.customer_id = s.customer_id
),
ranked AS (
  SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY composite_risk_score DESC) AS risk_percentile
  FROM risk_base
)
SELECT
  customer_id,
  segment_label,
  risk_band,
  ROUND(fraud_rate, 6) AS fraud_rate,
  ROUND(high_value_ratio, 6) AS high_value_ratio,
  ROUND(merchant_exposure_ratio, 6) AS merchant_exposure_ratio,
  ROUND(velocity_7d_30d_ratio, 6) AS velocity_7d_30d_ratio,
  ROUND(monetary, 2) AS monetary,
  ROUND(composite_risk_score, 6) AS composite_risk_score,
  ROUND(risk_percentile, 6) AS risk_percentile
FROM ranked
WHERE risk_band IN ('HIGH', 'MEDIUM') OR segment_label = 'HIGH_RISK'
ORDER BY composite_risk_score DESC;


-- 5) Dormant customer analysis
WITH dormant AS (
  SELECT
    s.customer_id,
    s.segment_label,
    s.risk_band,
    f.recency_days,
    f.txns_30d,
    f.monetary,
    f.net_flow,
    CASE
      WHEN f.recency_days BETWEEN 91 AND 180 THEN 'DORMANT_91_180'
      WHEN f.recency_days BETWEEN 181 AND 365 THEN 'DORMANT_181_365'
      WHEN f.recency_days > 365 THEN 'DORMANT_365_PLUS'
      ELSE 'NOT_DORMANT'
    END AS dormant_bucket
  FROM mart_customer_segments s
  JOIN mart_customer_features f ON f.customer_id = s.customer_id
  WHERE s.segment_label = 'DORMANT' OR (f.recency_days > 90 AND f.txns_30d = 0)
),
summary AS (
  SELECT
    dormant_bucket,
    COUNT(*) AS dormant_customers,
    SUM(monetary) AS dormant_value,
    AVG(monetary) AS avg_dormant_value,
    AVG(CASE WHEN risk_band = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_ratio
  FROM dormant
  GROUP BY dormant_bucket
)
SELECT
  dormant_bucket,
  dormant_customers,
  ROUND(dormant_value, 2) AS dormant_value,
  ROUND(avg_dormant_value, 2) AS avg_dormant_value,
  ROUND(high_risk_ratio, 6) AS high_risk_ratio
FROM summary
ORDER BY dormant_customers DESC;
