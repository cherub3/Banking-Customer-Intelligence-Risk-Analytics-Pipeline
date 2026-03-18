-- MySQL 8+
-- Stage 1: Profiling + Cleaning + Staging

-- Profiling
SELECT COUNT(*) AS total_rows FROM paysim_raw;

SELECT
  SUM(step IS NULL) AS step_nulls,
  SUM(type IS NULL OR TRIM(type)='') AS type_nulls,
  SUM(amount IS NULL) AS amount_nulls,
  SUM(nameOrig IS NULL OR TRIM(nameOrig)='') AS nameOrig_nulls,
  SUM(oldbalanceOrg IS NULL) AS oldbalanceOrg_nulls,
  SUM(newbalanceOrig IS NULL) AS newbalanceOrig_nulls,
  SUM(nameDest IS NULL OR TRIM(nameDest)='') AS nameDest_nulls,
  SUM(oldbalanceDest IS NULL) AS oldbalanceDest_nulls,
  SUM(newbalanceDest IS NULL) AS newbalanceDest_nulls,
  SUM(isFraud IS NULL) AS isFraud_nulls,
  SUM(isFlaggedFraud IS NULL) AS isFlaggedFraud_nulls
FROM paysim_raw;

SELECT
  COUNT(*) AS duplicate_groups,
  COALESCE(SUM(cnt - 1), 0) AS duplicate_rows
FROM (
  SELECT COUNT(*) AS cnt
  FROM paysim_raw
  GROUP BY step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
  HAVING COUNT(*) > 1
) d;

WITH stats AS (
  SELECT AVG(amount) AS mean_amt, STDDEV_POP(amount) AS std_amt
  FROM paysim_raw
  WHERE amount IS NOT NULL
)
SELECT COUNT(*) AS amount_outlier_rows
FROM paysim_raw p
CROSS JOIN stats s
WHERE s.std_amt > 0
  AND ABS((p.amount - s.mean_amt) / s.std_amt) > 3;

-- Staging tables
DROP TABLE IF EXISTS staging_transactions;
CREATE TABLE staging_transactions (
  transaction_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  step INT NOT NULL,
  type VARCHAR(20) NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  nameOrig VARCHAR(64) NOT NULL,
  oldbalanceOrg DECIMAL(18,2) NOT NULL,
  newbalanceOrig DECIMAL(18,2) NOT NULL,
  nameDest VARCHAR(64) NOT NULL,
  oldbalanceDest DECIMAL(18,2) NOT NULL,
  newbalanceDest DECIMAL(18,2) NOT NULL,
  isFraud TINYINT(1) NOT NULL,
  isFlaggedFraud TINYINT(1) NOT NULL DEFAULT 0,
  source_hash CHAR(64) NOT NULL,
  UNIQUE KEY uq_staging_source_hash (source_hash)
);

DROP TABLE IF EXISTS staging_transactions_rejects;
CREATE TABLE staging_transactions_rejects (
  reject_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  step INT,
  type VARCHAR(20),
  amount DECIMAL(18,2),
  nameOrig VARCHAR(64),
  oldbalanceOrg DECIMAL(18,2),
  newbalanceOrig DECIMAL(18,2),
  nameDest VARCHAR(64),
  oldbalanceDest DECIMAL(18,2),
  newbalanceDest DECIMAL(18,2),
  isFraud TINYINT,
  isFlaggedFraud TINYINT,
  reject_reason VARCHAR(64),
  source_hash CHAR(64)
);

-- Full-row exact de-dup only
DROP TEMPORARY TABLE IF EXISTS tmp_paysim_dedup;
CREATE TEMPORARY TABLE tmp_paysim_dedup AS
SELECT DISTINCT
  step,
  UPPER(TRIM(type)) AS type,
  amount,
  TRIM(nameOrig) AS nameOrig,
  oldbalanceOrg,
  newbalanceOrig,
  TRIM(nameDest) AS nameDest,
  oldbalanceDest,
  newbalanceDest,
  isFraud,
  COALESCE(isFlaggedFraud, 0) AS isFlaggedFraud
FROM paysim_raw;

DROP TEMPORARY TABLE IF EXISTS tmp_paysim_validated;
CREATE TEMPORARY TABLE tmp_paysim_validated AS
SELECT
  d.step,
  d.type,
  d.amount,
  d.nameOrig,
  d.oldbalanceOrg,
  d.newbalanceOrig,
  d.nameDest,
  d.oldbalanceDest,
  d.newbalanceDest,
  d.isFraud,
  d.isFlaggedFraud,
  SHA2(CONCAT_WS('|',d.step,d.type,d.amount,d.nameOrig,d.oldbalanceOrg,d.newbalanceOrig,d.nameDest,d.oldbalanceDest,d.newbalanceDest,d.isFraud,d.isFlaggedFraud), 256) AS source_hash,
  CASE
    WHEN d.step IS NULL OR d.type IS NULL OR d.amount IS NULL OR d.nameOrig IS NULL OR d.nameDest IS NULL
      OR d.oldbalanceOrg IS NULL OR d.newbalanceOrig IS NULL OR d.oldbalanceDest IS NULL OR d.newbalanceDest IS NULL OR d.isFraud IS NULL
      OR d.nameOrig = '' OR d.nameDest = '' THEN 'MISSING_CRITICAL'
    WHEN d.type NOT IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN') THEN 'INVALID_TYPE'
    WHEN d.amount <= 0 THEN 'NON_POSITIVE_AMOUNT'
    WHEN d.oldbalanceOrg < 0 OR d.newbalanceOrig < 0 OR d.oldbalanceDest < 0 OR d.newbalanceDest < 0 THEN 'NEGATIVE_BALANCE'
    WHEN d.nameOrig = d.nameDest THEN 'SAME_SENDER_RECEIVER'
    WHEN d.type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT') AND d.oldbalanceOrg < d.amount THEN 'INSUFFICIENT_FUNDS'
    WHEN d.type = 'CASH_IN'
         AND ABS((d.oldbalanceOrg + d.amount) - d.newbalanceOrig) > 0.000001 THEN 'SENDER_BALANCE_MISMATCH'
    WHEN d.type IN ('PAYMENT','TRANSFER','CASH_OUT','DEBIT')
         AND ABS((d.oldbalanceOrg - d.amount) - d.newbalanceOrig) > 0.000001 THEN 'SENDER_BALANCE_MISMATCH'
    WHEN d.type IN ('TRANSFER','CASH_IN')
         AND ABS((d.oldbalanceDest + d.amount) - d.newbalanceDest) > 0.000001 THEN 'RECEIVER_BALANCE_MISMATCH'
    ELSE NULL
  END AS reject_reason
FROM tmp_paysim_dedup d;

INSERT INTO staging_transactions_rejects (
  step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,
  isFraud,isFlaggedFraud,reject_reason,source_hash
)
SELECT
  step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,
  isFraud,isFlaggedFraud,reject_reason,source_hash
FROM tmp_paysim_validated
WHERE reject_reason IS NOT NULL;

INSERT INTO staging_transactions (
  step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,
  isFraud,isFlaggedFraud,source_hash
)
SELECT
  step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,
  isFraud,isFlaggedFraud,source_hash
FROM tmp_paysim_validated
WHERE reject_reason IS NULL;

SELECT COUNT(*) AS staging_transactions_count FROM staging_transactions;
SELECT COUNT(*) AS staging_transactions_rejects_count FROM staging_transactions_rejects;
