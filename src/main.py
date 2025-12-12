import sys
import os
import traceback
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from src.utils import setup_logger
from src.ingestion import FileIngestor
from src.validation import Validator
from src.standardization import Standardizer
from src.transformation import Transformer
from src.loader import Loader

logger = setup_logger('main_pipeline')

def run_pipeline(source_folder='data', file_pattern='*'):
    """
    Runs the ETL pipeline for files in source_folder.
    """
    logger.info(f"Starting ETL Pipeline. Source: {source_folder}, Pattern: {file_pattern}")
    
    # 1. INGESTION
    # ---------------------------------------------------------
    ingestor = FileIngestor()
    logger.info("Step 1: Ingestion")
    df = ingestor.read_folder(source_folder, file_pattern=file_pattern)
    
    if df.empty:
        logger.warning("No data found! Check 'data' folder.")
        return

    # 2. STANDARDIZATION (Column Names)
    # ---------------------------------------------------------
    logger.info("Step 2: Standardization")
    df = Standardizer.standardize_columns(df)
    
    # Standardizer usually lowercases and underscores.
    # Source: "Customer First Name" -> "customer_first_name"
    # Source: "Maritial_Status" -> "maritial_status" (Note typo in source)
    
    # 3. VALIDATION
    # ---------------------------------------------------------
    logger.info("Step 3: Validation")
    # Check if we have essential columns (even if names vary slightly, we validate after mapping usually, but here we do soft check)
    if 'premium_amt' not in df.columns and 'premium_amount' not in df.columns:
        logger.warning("Critical columns missing (premium logic might fail).")

    # 4. TRANSFORMATION & MAPPING
    # ---------------------------------------------------------
    logger.info("Step 4: Transformation & Cleaning")

    # 4.1 Construct Customer Name
    # Source has Title, First, Middle, Last. Target needs 'customer_name'.
    # Handle NaNs to avoid "Nan" string
    name_cols = ['customer_title', 'customer_first_name', 'customer_middle_name', 'customer_last_name']
    for c in name_cols:
        if c in df.columns:
            df[c] = df[c].fillna('').astype(str).replace('nan', '')
    
    # Combine if parts exist
    if 'customer_first_name' in df.columns:
         df['customer_name'] = (
            df['customer_title'] + " " + 
            df['customer_first_name'] + " " + 
            df['customer_middle_name'] + " " + 
            df['customer_last_name']
        ).str.replace(r'\s+', ' ', regex=True).str.strip()
    
    # 4.2 Rename/Map Columns
    rename_map = {
        'policy_type': 'policy_type_name', # Map Schema "Policy Type" Name to Dim Column
        'state_or_province': 'state_province',
        'postal_code': 'postal_code',
        'maritial_status': 'marital_status' # Fix source typo
    }
    df = df.rename(columns=rename_map)

    # 4.3 Clean Strings
    df = Standardizer.trim_strings(df)
    
    # 4.4 Format Dates
    date_cols = [
        'dob', 'effective_start_dt', 'effective_end_dt', 
        'policy_start_dt', 'policy_end_dt', 
        'next_premium_dt', 'actual_premium_paid_dt', 'txn_date'
    ]
    df = Standardizer.parse_dates(df, cols=date_cols)

    # 4.5 Clean Currency
    currency_cols = ['premium_amt', 'total_policy_amt', 'premium_amt_paid_tilldate', 'payment_amount']
    df = Standardizer.clean_currency(df, cols=currency_cols)
    
    # 4.6 Calculate Late Fees
    # Needs: actual_premium_paid_dt, next_premium_dt, premium_amt
    df = Transformer.calculate_late_fees(df)

    # 5. PREPARE FOR STAGING
    # ---------------------------------------------------------
    # Add Meta
    df['ingestion_date'] = datetime.now()
    if 'source_file' not in df.columns:
        df['source_file'] = 'unknown'
        
    # Transaction Date & Amount Logic
    # Staging Transactions needs 'txn_date' and 'premium_amount'
    if 'actual_premium_paid_dt' in df.columns:
        df['txn_date'] = df['actual_premium_paid_dt']
    
    # Map premium_amt -> premium_amount for Staging Transaction
    if 'premium_amt' in df.columns:
        df['premium_amount'] = df['premium_amt']
    
    # Map premium_amt_paid_tilldate -> premium_paid_tilldate
    if 'premium_amt_paid_tilldate' in df.columns:
        df['premium_paid_tilldate'] = df['premium_amt_paid_tilldate']


    # 6. LOADING (Staging Layer)
    # ---------------------------------------------------------
    logger.info("Step 6: Loading Staging Layer")
    loader = Loader(db_type='mysql')
    
    # Staging Customer
    # We include all available columns that match Staging DDL
    stg_cust = Transformer.split_dataframe(df, [
        'customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 
        'dob', 'effective_start_dt', 'effective_end_dt', 'region', 'ingestion_date', 'source_file'
    ])
    
    # Staging Policy
    # We map 'policy_type_name' back to 'policy_type' if needed by Staging Schema, or keep as is.
    # Staging DDL has 'policy_type'. We renamed source 'policy_type' -> 'policy_type_name'.
    # Let's provide 'policy_type' key for Staging load.
    df['policy_type'] = df.get('policy_type_name') 
    
    stg_pol = Transformer.split_dataframe(df, [
        'policy_id', 'policy_name', 'policy_type_id', 'policy_type', 'policy_type_desc', 
        'policy_term', 'policy_start_dt', 'policy_end_dt', 'total_policy_amt', 'premium_amt', 
        'next_premium_dt', 'actual_premium_paid_dt', 'premium_amt_paid_tilldate', 
        'customer_id', 'region', 'ingestion_date', 'source_file'
    ])

    # Staging Address
    stg_addr = Transformer.split_dataframe(df, [
        'customer_id', 'country', 'region', 'state_province', 'city', 'postal_code', 
        'ingestion_date', 'source_file'
    ])
    
    # Staging Transactions
    stg_txn = Transformer.split_dataframe(df, [
        'policy_id', 'customer_id', 'txn_date', 'premium_amount', 'premium_paid_tilldate', 
        'total_policy_amt', 'region', 'ingestion_date', 'source_file'
    ])

    # Load Staging (Append Mode)
    loader.load_to_db(stg_cust, 'stg_customers', if_exists='append')
    loader.load_to_db(stg_pol, 'stg_policy', if_exists='append')
    loader.load_to_db(stg_addr, 'stg_address', if_exists='append')
    loader.load_to_db(stg_txn, 'stg_transactions', if_exists='append')


    # 7. LOADING (DWH Layer)
    # ---------------------------------------------------------
    logger.info("Step 7: Loading DWH Layer (Dimensions & Facts)")

    # 7.1 Dim Policy Type
    dim_pol_type = df[['policy_type_id', 'policy_type_name', 'policy_type_desc']].drop_duplicates('policy_type_id')
    loader.load_to_db(dim_pol_type, 'dim_policy_type', if_exists='append')
    
    # 7.2 Dim Policy
    # Map 'policy_term' -> 'term', 'policy_start_dt' -> 'start_date', etc. based on DDL or keep strict.
    # Checking DDL (hybrid_schema_mysql.sql or staging_and_dwh.sql - user wants staging_and_dwh)
    # in staging_and_dwh.sql: dim_policy has columns: [policy_sk, policy_id, policy_name, policy_type_id, policy_type, policy_term, policy_start_dt, policy_end_dt, total_policy_amt, created_at]
    # So we DO NOT rename to 'term' or 'start_date'. We keep 'policy_term'.
    dim_policy = stg_pol[['policy_id', 'policy_name', 'policy_type_id', 'policy_type', 'policy_term', 'policy_start_dt', 'policy_end_dt', 'total_policy_amt']].drop_duplicates('policy_id')
    dim_policy['created_at'] = datetime.now()
    loader.load_to_db(dim_policy, 'dim_policy', if_exists='append')
    
    # 7.3 Dim Customer
    dim_cust = stg_cust[['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'dob', 'effective_start_dt', 'effective_end_dt', 'region']].drop_duplicates('customer_id')
    dim_cust['current_flag'] = 1
    dim_cust['created_at'] = datetime.now()
    # DDL also has 'eff_start_dt', 'eff_end_dt' ?
    # DDL: effective_start_dt -> eff_start_dt, effective_end_dt -> eff_end_dt
    dim_cust = dim_cust.rename(columns={'effective_start_dt': 'eff_start_dt', 'effective_end_dt': 'eff_end_dt'})
    loader.load_to_db(dim_cust, 'dim_customer', if_exists='append')

    # 7.4 Dim Address
    dim_addr = stg_addr[['customer_id', 'country', 'region', 'state_province', 'city', 'postal_code']].drop_duplicates(['customer_id', 'postal_code'])
    dim_addr['created_at'] = datetime.now()
    loader.load_to_db(dim_addr, 'dim_address', if_exists='append')
    
    # 7.5 Fact Policy Txn
    # DDL: fact_sk, customer_sk, policy_sk, address_sk, date_sk, premium_amt, premium_paid_tilldate, total_policy_amt, late_fee, region, ingestion_date
    # Note: Using Natural Keys for FKs temporarily as discussed.
    fact_tx = df[['customer_id', 'policy_id', 'txn_date', 'premium_amt', 'premium_paid_tilldate', 'total_policy_amt', 'late_fee_amount', 'region', 'ingestion_date']]
    fact_tx = fact_tx.rename(columns={'late_fee_amount': 'late_fee'})
    loader.load_to_db(fact_tx, 'fact_policy_txn', if_exists='append')

    logger.info("Pipeline Complete Successfully.")

if __name__ == "__main__":
    try:
        run_pipeline(source_folder='data')
    except Exception:
        traceback.print_exc()
