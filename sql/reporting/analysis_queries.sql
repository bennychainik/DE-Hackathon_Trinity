
-- =========================================================
-- ANALYSIS QUERIES
-- Based on User Request (Image)
-- =========================================================

-- b) Get list of customers who have changed their policy type along with current and previous policy type details.
-- Assumption: We track policy changes in dim_policy (SCD-like) or stg_policy history.
-- Logic: Self-join dim_policy on policy_id where policy_type differs and start_date is later.
-- Note: Simplified to show change.
SELECT 
    c.customer_id,
    c.customer_name,
    p_curr.policy_type AS current_policy_type,
    p_prev.policy_type AS previous_policy_type,
    p_curr.policy_id
FROM dim_policy p_curr
JOIN dim_policy p_prev ON p_curr.policy_id = p_prev.policy_id
    AND p_curr.policy_start_dt > p_prev.policy_start_dt
    AND p_curr.policy_type <> p_prev.policy_type
JOIN dim_customer c ON p_curr.customer_id = c.customer_id -- Note: dim_policy does not have customer_id in DDL?
-- WAIT: Check DDL for dim_policy.
-- DDL: dim_policy (policy_sk, policy_id, ..., policy_type, ...) NO CUSTOMER_ID in dim_policy!
-- Fact table links Customer and Policy. We must link via Fact or Staging?
-- Using Fact Table to link Policy to Customer for current status.
-- BUT, if we just want the policy change, we can find policy_ids first.
-- To get Customer Name, we need to join back to Customer.
-- Correct Path: dim_policy (find change) -> fact_policy_txn (link to customer_sk) -> dim_customer.
-- Or if fact_policy_txn is too granular, we assume stg_policy or standard Snowflake.
-- Let's use Fact to link relevant customer.
LEFT JOIN fact_policy_txn f ON p_curr.policy_sk = f.policy_sk
LEFT JOIN dim_customer dc ON f.customer_sk = dc.customer_sk
WHERE dc.current_flag = 1 
GROUP BY c.customer_id, c.customer_name, p_curr.policy_type, p_prev.policy_type, p_curr.policy_id;

-- Wait, DDL Check:
-- dim_policy does NOT have customer_id.
-- stg_policy HAS customer_id.
-- fact_policy_txn HAS customer_sk and policy_sk.
-- So join path: dim_policy p1, dim_policy p2 -> fact -> dim_customer.

-- REVISED QUERY (b)
SELECT DISTINCT
    dc.customer_id,
    dc.customer_name,
    p_curr.policy_type AS current_policy_type,
    p_curr.policy_id AS current_policy_id,
    p_curr.policy_name AS current_policy_name,
    p_prev.policy_type AS previous_policy_type,
    p_prev.policy_id AS previous_policy_id,
    p_prev.policy_name AS previous_policy_name
FROM dim_policy p_curr
JOIN dim_policy p_prev 
    ON p_curr.policy_id = p_prev.policy_id 
    AND p_curr.policy_start_dt > p_prev.policy_start_dt
    AND p_curr.policy_type != p_prev.policy_type
JOIN fact_policy_txn f ON f.policy_sk = p_curr.policy_sk
JOIN dim_customer dc ON f.customer_sk = dc.customer_sk;


-- c) Total policy amount by all customers and all regions.
-- "Region" from Customer Dimension (or Address).
-- Sum of 'total_policy_amt'.
-- Avoid double counting if using Fact (transactions). 
-- dim_policy has 'total_policy_amt'.
-- Linked via Fact.
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.region,
    SUM(dp.total_policy_amt) AS Total_Policy_Amt
FROM dim_customer dc
JOIN fact_policy_txn f ON dc.customer_sk = f.customer_sk
JOIN dim_policy dp ON f.policy_sk = dp.policy_sk
WHERE dc.current_flag = 1 -- Use current customer details
GROUP BY dc.customer_id, dc.customer_name, dc.region;


-- d) Total policy amount by all customers having policy type as Auto.
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.region,
    dp.policy_type,
    SUM(dp.total_policy_amt) AS Total_Policy_Amt
FROM dim_customer dc
JOIN fact_policy_txn f ON dc.customer_sk = f.customer_sk
JOIN dim_policy dp ON f.policy_sk = dp.policy_sk
WHERE dp.policy_type = 'Auto'
  AND dc.current_flag = 1
GROUP BY dc.customer_id, dc.customer_name, dc.region, dp.policy_type;


-- e) Total policy amount by east and west customers having quarterly policy term for year 2012.
SELECT 
    dc.customer_id,
    dc.customer_name,
    dc.region,
    dp.policy_term,
    dp.policy_start_dt,
    SUM(dp.total_policy_amt) AS Total_Policy_Amt
FROM dim_customer dc
JOIN fact_policy_txn f ON dc.customer_sk = f.customer_sk
JOIN dim_policy dp ON f.policy_sk = dp.policy_sk
WHERE dc.region IN ('East', 'West')
  AND dp.policy_term = 'Quarterly'
  AND YEAR(dp.policy_start_dt) = 2012
  AND dc.current_flag = 1
GROUP BY dc.customer_id, dc.customer_name, dc.region, dp.policy_term, dp.policy_start_dt;


-- f) Get list of customers whose marital status has changed.
-- Using SCD Type 2 table dim_customer.
-- Look for customer_ids with multiple rows having different marital_status.
SELECT 
    c1.customer_id,
    c1.customer_name AS customer_title, -- Using name as placeholder, DDL doesn't show Title column separately, assumed in Name.
    c1.customer_name AS customer_first_name,
    c1.customer_name AS customer_last_name,
    c1.customer_segment,
    c1.marital_status,
    c1.eff_start_dt AS start_dt,
    c1.eff_end_dt AS end_dt
FROM dim_customer c1
WHERE c1.customer_id IN (
    SELECT customer_id
    FROM dim_customer
    GROUP BY customer_id
    HAVING COUNT(DISTINCT marital_status) > 1
)
ORDER BY c1.customer_id, c1.eff_start_dt;


-- g) Display all regions' customer data along with their policy, policy type and address information and policy details.
SELECT 
    dc.region,
    dc.customer_id,
    dc.customer_name,
    dc.customer_segment,
    dc.marital_status,
    dc.gender,
    da.city,
    da.state_province,
    da.postal_code,
    dp.policy_id,
    dp.policy_name,
    dp.policy_type,
    dpt.policy_type_desc,
    dp.policy_term,
    dp.policy_start_dt,
    dp.policy_end_dt,
    dp.total_policy_amt
FROM dim_customer dc
JOIN fact_policy_txn f On dc.customer_sk = f.customer_sk
JOIN dim_policy dp ON f.policy_sk = dp.policy_sk
LEFT JOIN dim_policy_type dpt ON dp.policy_type_id = dpt.policy_type_id
LEFT JOIN dim_address da ON f.address_sk = da.address_sk
WHERE dc.current_flag = 1;

-- h) Get list of addresses changed by a customer in a given time frame.
-- Strategy: Find distinct addresses used in Fact table within the date range.
-- If a customer appears with multiple different address_sks, they moved.
SELECT DISTINCT
    dc.customer_id,
    dc.customer_name,
    da.address_sk,
    da.city,
    da.state_province,
    da.postal_code,
    da.country,
    MIN(f.txn_date) as first_seen_date,
    MAX(f.txn_date) as last_seen_date
FROM fact_policy_txn f
JOIN dim_address da ON f.address_sk = da.address_sk
JOIN dim_customer dc ON f.customer_sk = dc.customer_sk
-- Filter by date range (placeholders)
WHERE f.txn_date BETWEEN '2010-01-01' AND '2025-12-31' 
GROUP BY dc.customer_id, dc.customer_name, da.address_sk, da.city, da.state_province, da.postal_code, da.country
ORDER BY dc.customer_id, first_seen_date;

