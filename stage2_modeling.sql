DROP TABLE IF EXISTS fact_transaction;
DROP TABLE IF EXISTS dim_account;
DROP TABLE IF EXISTS dim_customer;

CREATE TABLE dim_customer (
  customer_id CHAR(19) PRIMARY KEY,
  external_party_ref VARCHAR(64) NOT NULL UNIQUE,
  customer_type ENUM('RETAIL','MERCHANT','UNKNOWN') NOT NULL,
  source_system VARCHAR(32) NOT NULL DEFAULT 'PAYSIM',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE dim_account (
  account_id CHAR(19) PRIMARY KEY,
  account_number VARCHAR(64) NOT NULL UNIQUE,
  customer_id CHAR(19) NOT NULL,
  account_type ENUM('CUSTOMER','MERCHANT','UNKNOWN') NOT NULL,
  account_status ENUM('ACTIVE','INACTIVE') NOT NULL DEFAULT 'ACTIVE',
  open_step INT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  INDEX idx_dim_account_customer_id (customer_id),
  CONSTRAINT fk_dim_account_customer
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
) ENGINE=InnoDB;

CREATE TABLE fact_transaction (
  transaction_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_hash CHAR(64) NOT NULL UNIQUE,
  step INT NOT NULL,
  transaction_type ENUM('PAYMENT','TRANSFER','CASH_OUT','DEBIT','CASH_IN') NOT NULL,
  amount DECIMAL(18,2) NOT NULL,
  origin_account_id CHAR(19) NOT NULL,
  destination_account_id CHAR(19) NOT NULL,
  oldbalanceOrg DECIMAL(18,2) NOT NULL,
  newbalanceOrig DECIMAL(18,2) NOT NULL,
  oldbalanceDest DECIMAL(18,2) NOT NULL,
  newbalanceDest DECIMAL(18,2) NOT NULL,
  isFraud TINYINT(1) NOT NULL,
  isFlaggedFraud TINYINT(1) NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  INDEX idx_fact_step (step),
  INDEX idx_fact_type (transaction_type),
  INDEX idx_fact_origin_account_id (origin_account_id),
  INDEX idx_fact_destination_account_id (destination_account_id),
  INDEX idx_fact_isfraud (isFraud),
  CONSTRAINT fk_fact_origin_account
    FOREIGN KEY (origin_account_id) REFERENCES dim_account(account_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT,
  CONSTRAINT fk_fact_dest_account
    FOREIGN KEY (destination_account_id) REFERENCES dim_account(account_id)
    ON UPDATE RESTRICT
    ON DELETE RESTRICT
) ENGINE=InnoDB;

DROP TEMPORARY TABLE IF EXISTS tmp_party;
CREATE TEMPORARY TABLE tmp_party AS
SELECT DISTINCT nameOrig AS party_ref FROM staging_transactions
UNION
SELECT DISTINCT nameDest AS party_ref FROM staging_transactions;

DROP TEMPORARY TABLE IF EXISTS tmp_party_open_step;
CREATE TEMPORARY TABLE tmp_party_open_step AS
SELECT ref AS party_ref, MIN(step) AS open_step
FROM (
  SELECT step, nameOrig AS ref FROM staging_transactions
  UNION ALL
  SELECT step, nameDest AS ref FROM staging_transactions
) t
GROUP BY ref;

INSERT INTO dim_customer (customer_id, external_party_ref, customer_type)
SELECT
  CONCAT('CU_', UPPER(SUBSTRING(SHA2(p.party_ref, 256), 1, 16))) AS customer_id,
  p.party_ref,
  CASE
    WHEN p.party_ref LIKE 'C%' THEN 'RETAIL'
    WHEN p.party_ref LIKE 'M%' THEN 'MERCHANT'
    ELSE 'UNKNOWN'
  END AS customer_type
FROM tmp_party p
LEFT JOIN dim_customer dc ON dc.external_party_ref = p.party_ref
WHERE dc.external_party_ref IS NULL;

INSERT INTO dim_account (account_id, account_number, customer_id, account_type, account_status, open_step)
SELECT
  CONCAT('AC_', UPPER(SUBSTRING(SHA2(p.party_ref, 256), 1, 16))) AS account_id,
  p.party_ref AS account_number,
  CONCAT('CU_', UPPER(SUBSTRING(SHA2(p.party_ref, 256), 1, 16))) AS customer_id,
  CASE
    WHEN p.party_ref LIKE 'C%' THEN 'CUSTOMER'
    WHEN p.party_ref LIKE 'M%' THEN 'MERCHANT'
    ELSE 'UNKNOWN'
  END AS account_type,
  'ACTIVE' AS account_status,
  os.open_step
FROM tmp_party p
LEFT JOIN tmp_party_open_step os ON os.party_ref = p.party_ref
LEFT JOIN dim_account da ON da.account_number = p.party_ref
WHERE da.account_number IS NULL;

INSERT INTO fact_transaction (
  source_hash, step, transaction_type, amount,
  origin_account_id, destination_account_id,
  oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
  isFraud, isFlaggedFraud
)
SELECT
  s.source_hash,
  s.step,
  s.type,
  s.amount,
  CONCAT('AC_', UPPER(SUBSTRING(SHA2(s.nameOrig, 256), 1, 16))) AS origin_account_id,
  CONCAT('AC_', UPPER(SUBSTRING(SHA2(s.nameDest, 256), 1, 16))) AS destination_account_id,
  s.oldbalanceOrg,
  s.newbalanceOrig,
  s.oldbalanceDest,
  s.newbalanceDest,
  s.isFraud,
  s.isFlaggedFraud
FROM staging_transactions s
LEFT JOIN fact_transaction f ON f.source_hash = s.source_hash
WHERE f.source_hash IS NULL;

SELECT COUNT(*) AS dim_customer_count FROM dim_customer;
SELECT COUNT(*) AS dim_account_count FROM dim_account;
SELECT COUNT(*) AS fact_transaction_count FROM fact_transaction;
