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
    df = Transformer.construct_customer_name(df)
    
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
    
    # 7.3 Dim Customer (SCD Type 2)
    # ---------------------------------------------
    # 1. Prepare Incoming (New) Data
    dim_cust_new = stg_cust[['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'region']].drop_duplicates('customer_id')
    # Use generic names for dates if not present
    
    # 2. Fetch Existing (Current) Data from DB
    sql_ingestor = FileIngestor() # We need SQL capabilities. Using 'src/ingestion.py' SQLIngestor helper if available or creating engine directly.
    # Actually Ingestion module has SQLIngestor class? Let's check imports.
    # We imported FileIngestor. We need SQLIngestor.
    # Let's import generic utils engine or use Loader's engine?
    from src.ingestion import SQLIngestor
    
    try:
        sql_reader = SQLIngestor(db_type='mysql')
        # We only compare against CURRENT records
        existing_cust = sql_reader.read_query("SELECT customer_sk, customer_id, customer_name, customer_segment, marital_status, region FROM dim_customer WHERE current_flag = 1")
    except Exception as e:
        logger.warning(f"Could not fetch existing DimCustomer (First Run?): {e}")
        existing_cust = pd.DataFrame()

    # 3. Apply SCD2 Logic
    # Compare columns: name, segment, marital, region
    compare_cols = ['customer_name', 'customer_segment', 'marital_status', 'region']
    
    if existing_cust.empty:
        # First Load -> All are new inserts
        to_insert = dim_cust_new.copy()
        to_insert['current_flag'] = 1
        to_insert['eff_start_dt'] = datetime.now().date()
        to_insert['eff_end_dt'] = '9999-12-31'
        to_update = pd.DataFrame()
    else:
        to_insert, to_update = Transformer.scd_type_2(
            new_df=dim_cust_new,
            existing_df=existing_cust,
            join_keys=['customer_id'],
            compare_cols=compare_cols
        )

    # 4. Perform Updates (Expire Old) inside DB
    if not to_update.empty:
        # We need to run SQL UPDATE statements. 
        # Loader generic 'load_to_db' is usually INSERT only.
        # We'll do a custom execute for updates.
        logger.info(f"Expiring {len(to_update)} old customer records...")
        from sqlalchemy import text
        with loader.engine.connect() as conn:
            for index, row in to_update.iterrows():
                # Update specific SK
                stmt = text(f"UPDATE dim_customer SET current_flag = 0, eff_end_dt = :end_dt WHERE customer_sk = :sk")
                conn.execute(stmt, {'end_dt': row['eff_end_dt'], 'sk': row['customer_sk']})
                conn.commit()

    # 5. Perform Inserts (New Rows)
    if not to_insert.empty:
        # Ensure columns match DDL
        # DDL: customer_id, customer_name, customer_segment, marital_status, gender, dob, eff_start_dt, eff_end_dt, current_flag, region, created_at
        # We are missing DOB in the comparison df above, we should merge it back from stg_cust if needed or include it earlier.
        # Simplification: We merge DOB back from stg_cust based on ID
        dob_lookup = stg_cust[['customer_id', 'dob']].drop_duplicates('customer_id')
        to_insert = pd.merge(to_insert, dob_lookup, on='customer_id', how='left')
        
        to_insert['created_at'] = datetime.now()
        # Ensure order/subset
        cols_to_load = ['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'dob', 'eff_start_dt', 'eff_end_dt', 'current_flag', 'region', 'created_at']
        # Handle columns that might not exist in to_insert if new_df didn't have them (e.g. gender)
        for c in cols_to_load:
            if c not in to_insert.columns:
                to_insert[c] = None # or map from source
                
        # Fix Gender: gender is in dim_cust_new? Yes.
        
        loader.load_to_db(to_insert[cols_to_load], 'dim_customer', if_exists='append')

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
