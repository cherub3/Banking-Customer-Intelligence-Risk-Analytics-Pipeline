-- MySQL 8+

DROP TABLE IF EXISTS paysim_raw;

CREATE TABLE paysim_raw (
  step INT,
  type VARCHAR(20),
  amount DOUBLE,
  nameOrig VARCHAR(64),
  oldbalanceOrg DOUBLE,
  newbalanceOrig DOUBLE,
  nameDest VARCHAR(64),
  oldbalanceDest DOUBLE,
  newbalanceDest DOUBLE,
  isFraud BIGINT,
  isFlaggedFraud BIGINT
);

-- Optional SQL import (if LOCAL INFILE enabled)
-- LOAD DATA LOCAL INFILE '/absolute/path/paysim.csv'
-- INTO TABLE paysim_raw
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES
-- (step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud);

SELECT COUNT(*) AS total_rows FROM paysim_raw;
