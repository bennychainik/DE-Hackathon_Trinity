-- INIT SCHEMA (MySQL)
-- Reusable Template for Fact-Dimension Modeling

CREATE DATABASE IF NOT EXISTS hackathon_dw;
USE hackathon_dw;

-- =============================================
-- Dimension Tables (SCD Type 1 Desumed)
-- =============================================

-- DimCustomer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk INT AUTO_INCREMENT PRIMARY KEY, -- Surrogate Key
    customer_id VARCHAR(50) NOT NULL UNIQUE,    -- Natural Key
    name VARCHAR(255),
    segment VARCHAR(100),
    email VARCHAR(255),
    active_flag BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- DimPolicy
CREATE TABLE IF NOT EXISTS dim_policy (
    policy_sk INT AUTO_INCREMENT PRIMARY KEY,
    policy_id VARCHAR(50) NOT NULL UNIQUE,
    policy_type VARCHAR(100),
    start_date DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DimAddress
CREATE TABLE IF NOT EXISTS dim_address (
    address_sk INT AUTO_INCREMENT PRIMARY KEY,
    region VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100) DEFAULT 'USA'
);

-- =============================================
-- Fact Table
-- =============================================

-- FactTransactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_sk INT,
    policy_sk INT,
    address_sk INT,
    transaction_date DATE,
    amount DECIMAL(15, 2),
    transaction_type VARCHAR(50),
    
    -- Foreign Keys
    FOREIGN KEY (customer_sk) REFERENCES dim_customer(customer_sk),
    FOREIGN KEY (policy_sk) REFERENCES dim_policy(policy_sk),
    FOREIGN KEY (address_sk) REFERENCES dim_address(address_sk)
);

-- Indexes for Performance
CREATE INDEX idx_fact_date ON fact_transactions(transaction_date);
CREATE INDEX idx_fact_cust ON fact_transactions(customer_sk);
