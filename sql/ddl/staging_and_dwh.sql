-- =========================================================
-- STAGING LAYER (Raw Standardized)
-- =========================================================
CREATE DATABASE IF NOT EXISTS insurance_dw;
USE insurance_dw;

-- Staging (append-only)
CREATE TABLE IF NOT EXISTS stg_customers (
  customer_id INT,
  customer_name VARCHAR(255),
  customer_segment VARCHAR(100),
  marital_status VARCHAR(50),
  gender VARCHAR(20),
  dob DATE,
  effective_start_dt DATE,
  effective_end_dt DATE,
  region VARCHAR(50),
  ingestion_date DATETIME,
  source_file VARCHAR(255)
  -- PRIMARY KEY (customer_id, source_file) -- optional
);

CREATE TABLE IF NOT EXISTS stg_policy (
  policy_id INT,
  policy_name VARCHAR(255),
  policy_type_id INT,
  policy_type VARCHAR(100),
  policy_type_desc TEXT,
  policy_term VARCHAR(50),
  policy_start_dt DATE,
  policy_end_dt DATE,
  total_policy_amt DECIMAL(18,2),
  premium_amt DECIMAL(18,2),
  next_premium_dt DATE,
  actual_premium_paid_dt DATE,
  premium_amt_paid_tilldate DECIMAL(18,2),
  customer_id INT,
  region VARCHAR(50),
  ingestion_date DATETIME,
  source_file VARCHAR(255)
  -- PRIMARY KEY (policy_id, source_file)
);

CREATE TABLE IF NOT EXISTS stg_address (
  address_id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  country VARCHAR(100),
  region VARCHAR(50),
  state_province VARCHAR(100),
  city VARCHAR(100),
  postal_code VARCHAR(50),
  ingestion_date DATETIME,
  source_file VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS stg_transactions (
  txn_id INT AUTO_INCREMENT PRIMARY KEY,
  policy_id INT,
  customer_id INT,
  txn_date DATE,
  premium_amount DECIMAL(18,2),
  premium_paid_tilldate DECIMAL(18,2),
  total_policy_amt DECIMAL(18,2),
  region VARCHAR(50),
  ingestion_date DATETIME,
  source_file VARCHAR(255)
);

-- =========================================================
-- WAREHOUSE LAYER (Dims & Facts)
-- =========================================================

-- Dimension tables (SCD2 for customers)
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  customer_name VARCHAR(255),
  customer_segment VARCHAR(100),
  marital_status VARCHAR(50),
  gender VARCHAR(20),
  dob DATE,
  eff_start_dt DATE,
  eff_end_dt DATE,
  current_flag TINYINT(1),
  region VARCHAR(50),
  created_at DATETIME,
  UNIQUE KEY (customer_id, eff_start_dt)
);

CREATE TABLE IF NOT EXISTS dim_policy_type (
  policy_type_id INT PRIMARY KEY,
  policy_type_name VARCHAR(100),
  policy_type_desc TEXT
);

CREATE TABLE IF NOT EXISTS dim_policy (
  policy_sk INT AUTO_INCREMENT PRIMARY KEY,
  policy_id INT,
  policy_name VARCHAR(255),
  policy_type_id INT,
  policy_type VARCHAR(100),
  policy_term VARCHAR(50),
  policy_start_dt DATE,
  policy_end_dt DATE,
  total_policy_amt DECIMAL(18,2),
  created_at DATETIME,
  UNIQUE KEY (policy_id, policy_start_dt)
);

CREATE TABLE IF NOT EXISTS dim_address (
  address_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  country VARCHAR(100),
  region VARCHAR(50),
  state_province VARCHAR(100),
  city VARCHAR(100),
  postal_code VARCHAR(50),
  created_at DATETIME
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_sk INT AUTO_INCREMENT PRIMARY KEY,
  dt DATE UNIQUE,
  year INT,
  month INT,
  day INT,
  quarter INT
);

-- Fact table
CREATE TABLE IF NOT EXISTS fact_policy_txn (
  fact_sk INT AUTO_INCREMENT PRIMARY KEY,
  customer_sk INT,
  policy_sk INT,
  address_sk INT,
  late_fee_sk INT, -- Link to Late Fee Dimension
  date_sk INT,
  premium_amt DECIMAL(18,2),
  premium_paid_tilldate DECIMAL(18,2),
  total_policy_amt DECIMAL(18,2),
  late_fee DECIMAL(18,2),
  region VARCHAR(50),
  ingestion_date DATETIME
);

-- Late Fee Dimension (Hybrid: Reference for rules)
CREATE TABLE IF NOT EXISTS dim_late_fee (
  late_fee_sk INT AUTO_INCREMENT PRIMARY KEY,
  duration_months INT UNIQUE, -- Key to identifying the rule
  penalty_percent DECIMAL(5,4), -- e.g. 0.025
  description VARCHAR(100), -- e.g. "5 Months Delay"
  created_at DATETIME
);
