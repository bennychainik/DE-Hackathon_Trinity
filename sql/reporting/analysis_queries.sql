-- =======================================================
-- SQL REPORTING TEMPLATES
-- =======================================================

-- 1. Aggregations by Region & Category (Generic)
SELECT 
    d_addr.region,
    d_pol.policy_type,
    SUM(f.amount) as total_amount,
    COUNT(DISTINCT f.customer_sk) as unique_customers
FROM fact_transactions f
JOIN dim_address d_addr ON f.address_sk = d_addr.address_sk
JOIN dim_policy d_pol ON f.policy_sk = d_pol.policy_sk
GROUP BY 1, 2
ORDER BY 1, 2;

-- 2. Detect Policy Type Changes (Using Window Functions)
-- Assumes we have a history table or effective dates, 
-- This example checks if a customer has different Active policies over time
WITH PolicyChanges AS (
    SELECT 
        customer_sk,
        policy_sk,
        transaction_date,
        LAG(policy_sk) OVER (PARTITION BY customer_sk ORDER BY transaction_date) as prev_policy_sk
    FROM fact_transactions
)
SELECT * 
FROM PolicyChanges
WHERE prev_policy_sk IS NOT NULL AND policy_sk <> prev_policy_sk;

-- 3. Date Range Filtering (Yearly/Quarterly)
SELECT 
    YEAR(f.transaction_date) as yr,
    QUARTER(f.transaction_date) as qtr,
    SUM(f.amount) as total_sales
FROM fact_transactions f
WHERE f.transaction_date BETWEEN '2012-01-01' AND '2012-12-31'
GROUP BY 1, 2;

-- 4. Ranking Customers (Window Function)
SELECT 
    d_cust.name,
    SUM(f.amount) as total_spend,
    RANK() OVER (ORDER BY SUM(f.amount) DESC) as rank_val
FROM fact_transactions f
JOIN dim_customer d_cust ON f.customer_sk = d_cust.customer_sk
GROUP BY 1;
