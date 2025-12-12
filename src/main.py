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

    # 4.2.1 Enforce String Types for IDs (Prevent Duplicates in MySQL due to Mixed Types)
    if 'policy_id' in df.columns:
        df['policy_id'] = df['policy_id'].fillna('').astype(str).str.strip()
    if 'policy_type_id' in df.columns:
        df['policy_type_id'] = df['policy_type_id'].fillna('').astype(str).str.strip()
    if 'customer_id' in df.columns:
        df['customer_id'] = df['customer_id'].fillna('').astype(str).str.strip() # Ensure consistency

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
    dim_cust_new = stg_cust[['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'region', 'effective_start_dt']].drop_duplicates('customer_id')
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
        # Use provided start date if available, else distinct past date for initial load
        if 'effective_start_dt' in to_insert.columns:
            to_insert['eff_start_dt'] = pd.to_datetime(to_insert['effective_start_dt']).fillna(datetime(1900, 1, 1).date())
        else:
            to_insert['eff_start_dt'] = datetime(1900, 1, 1).date()
            
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
        # Merge DOB and other missing static fields back from Staging
        dob_lookup = stg_cust[['customer_id', 'dob']].drop_duplicates('customer_id')
        to_insert = pd.merge(to_insert, dob_lookup, on='customer_id', how='left')
        
        to_insert['created_at'] = datetime.now()
        # Ensure order/subset
        cols_to_load = ['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'dob', 'eff_start_dt', 'eff_end_dt', 'current_flag', 'region', 'created_at']
        
        # Handle columns that might not exist in to_insert
        for c in cols_to_load:
            if c not in to_insert.columns:
                to_insert[c] = None 
        
        # Loader handles column mapping if strictly defined, but ensuring subset is good practice to avoid errors.
        loader.load_to_db(to_insert[cols_to_load], 'dim_customer', if_exists='append')

    # 7.4 Dim Address
    dim_addr = stg_addr[['customer_id', 'country', 'region', 'state_province', 'city', 'postal_code']].drop_duplicates(['customer_id', 'postal_code'])
    dim_addr['created_at'] = datetime.now()
    loader.load_to_db(dim_addr, 'dim_address', if_exists='append')
    
    # 7.5 Dim Late Fee (New Hybrid Dim)
    # Generate reference table 0-60 months
    fee_range = range(0, 61)
    dim_late_fee = pd.DataFrame({'duration_months': fee_range})
    dim_late_fee['penalty_percent'] = dim_late_fee['duration_months'] * 0.005
    dim_late_fee['description'] = dim_late_fee['duration_months'].astype(str) + " Months Delay"
    dim_late_fee['created_at'] = datetime.now()
    
    # Check if exists to avoid dupes (simple hack: try-except or just append if table empty)
    # For now, we append. If PK exists, it fails, which is fine for repeated runs (it stays populated).
    # To be cleaner, we can catch the error or check count.
    try:
        loader.load_to_db(dim_late_fee, 'dim_late_fee', if_exists='append') 
    except Exception:
        logger.warning("Dim Late Fee load failed (likely duplicates), skipping.")

    # 7.6 Fact Policy Txn with Surrogate Keys (SK) Lookup
    # ---------------------------------------------------
    from src.ingestion import SQLIngestor
    sql_reader = SQLIngestor(db_type='mysql')

    # A. Fetch Dimension Maps (ID -> SK)
    try:
        # Customer Map (Active records only for now? Or join on date? User schema is SCD2)
        # For simplicity in logic: Join on Customer ID and Region/Date? 
        # Ideally Fact joins on specific version. Here we just take 'current' or join on ID + Date range.
        # Strict SCD2 linkage requires: Fact.TxnDate BETWEEN Dim.Start AND Dim.End
        # We will fetch ALL history and do a merge_asof or careful merge.
        # Simplified for Hackathon: Fetch All, Merge on ID, Filter by Date.
        
        # Customer SK Map
        # Fetch: customer_sk, customer_id, eff_start_dt, eff_end_dt
        map_cust = sql_reader.read_query("SELECT customer_sk, customer_id, eff_start_dt, eff_end_dt FROM dim_customer")
        
        # Policy SK Map (Type 1 dim usually, or Type 2?) User said only Customer is SCD2.
        # Dim Policy is Type 1 (unique policy_id).
        map_pol = sql_reader.read_query("SELECT policy_sk, policy_id FROM dim_policy")
        
        # Address SK Map
        # Linked by customer_id (and postal code?)
        map_addr = sql_reader.read_query("SELECT address_sk, customer_id, postal_code FROM dim_address")
        
        # Late Fee SK Map
        map_fee = sql_reader.read_query("SELECT late_fee_sk, duration_months FROM dim_late_fee")
        
    except Exception as e:
        logger.error(f"Failed to fetch dimension maps for Fact linking: {e}")
        return

    # B. Map Customer SK (SCD Type 2 Linkage)
    # Ensure types match for merge (DB returns valid types, DF has strings)
    # Map Cust
    map_cust['customer_id'] = map_cust['customer_id'].astype(str)
    
    # Map Pol
    map_pol['policy_id'] = map_pol['policy_id'].astype(str)
    
    # Map Addr
    map_addr['customer_id'] = map_addr['customer_id'].astype(str)
    if 'postal_code' in map_addr.columns:
        map_addr['postal_code'] = map_addr['postal_code'].astype(str)
        
    # We need to match df['customer_id'] == map_cust['customer_id'] AND df['txn_date'] BETWEEN Start AND End.
    # Merge on ID
    df_merged = pd.merge(df, map_cust, on='customer_id', how='left')
    
    # Ensure Dates are datetime
    df_merged['txn_date'] = pd.to_datetime(df_merged['txn_date'], errors='coerce')
    df_merged['eff_start_dt'] = pd.to_datetime(df_merged['eff_start_dt'], errors='coerce')
    
    # Handle '9999' which breaks pd.to_datetime on some systems (Out of bounds)
    # We force errors='coerce' so 9999 becomes NaT, then replace with Max Timestamp
    df_merged['eff_end_dt'] = pd.to_datetime(df_merged['eff_end_dt'], errors='coerce')
    df_merged['eff_end_dt'] = df_merged['eff_end_dt'].fillna(pd.Timestamp.max)
    # Also handle strings if they survived
    
    # Filter: eff_start <= txn_date < eff_end
    # Note: If txn_date is missing, we fail match.
    mask = (df_merged['txn_date'] >= df_merged['eff_start_dt']) & (df_merged['txn_date'] <= df_merged['eff_end_dt'])
    
    # Keep only valid matches
    # If a customer has multiple history rows, 'merge' created duplicates. 'mask' identifies the correct one.
    # If no match found (txn before customer start?), we might get empty. Use Left Join logic carefully.
    
    # Better approach: Sort and drop duplicates or select valid.
    fact_data = df_merged[mask].copy()
    
    # If we lost rows (no matching Time slice), we should log.
    if len(fact_data) < len(df):
        logger.warning(f"SCD2 Linkage: Dropped {len(df) - len(fact_data)} rows due to date mismatch. (Check txn_date vs customer history).")
        # Optional: Recover them with 'Current' flag if robust logic needed.

    # C. Map Policy SK
    if 'policy_id' in fact_data.columns and 'policy_id' in map_pol.columns:
        # Ensure string type matching
        fact_data['policy_id'] = fact_data['policy_id'].astype(str)
        
        fact_data = pd.merge(fact_data, map_pol, on='policy_id', how='left')

    # D. Map Address SK
    # Join on Customer ID and Postal Code?
    if 'postal_code' in fact_data.columns:
        # Cast DF as well
        fact_data['postal_code'] = fact_data['postal_code'].fillna('').astype(str).replace(r'\.0$', '', regex=True)
        fact_data = pd.merge(fact_data, map_addr, on=['customer_id', 'postal_code'], how='left')
    elif 'customer_id' in fact_data.columns:
         # Fallback just customer
         fact_data = pd.merge(fact_data, map_addr, on='customer_id', how='left')

    # E. Map Late Fee SK
    if 'late_duration_months' in fact_data.columns:
        fact_data = pd.merge(fact_data, map_fee, left_on='late_duration_months', right_on='duration_months', how='left')

    # F. Map Date SK (Optional/Missing in logic) - User has dim_date but we don't have usage logic. 
    # Use 20251212 int format or lookup.
    # Skip for now if code didn't have it. DDL has `date_sk`. We should probably supply it.
    # Simple INT: YYYYMMDD
    fact_data['date_sk'] = fact_data['txn_date'].dt.strftime('%Y%m%d').fillna(0).astype(int)

    # G. Final Selection
    # Needed: fact_sk (Auto), customer_sk, policy_sk, address_sk, late_fee_sk, date_sk, premium_amt, premium_paid_tilldate, total_policy_amt, late_fee, region, ingestion_date
    final_cols = [
        'customer_sk', 'policy_sk', 'address_sk', 'late_fee_sk', 'date_sk', 
        'premium_amt', 'premium_paid_tilldate', 'total_policy_amt', 'late_fee', 'region', 'ingestion_date'
    ] # Corrected 'late_fee' to match DB column name
    
    # Rename late_fee_amount
    fact_data = fact_data.rename(columns={'late_fee_amount': 'late_fee'})
    
    # Add missing cols with NaN if merge failed
    for c in final_cols:
        if c not in fact_data.columns:
            fact_data[c] = None

    loader.load_to_db(fact_data[final_cols], 'fact_policy_txn', if_exists='append')

    logger.info("Pipeline Complete Successfully.")

if __name__ == "__main__":
    run_pipeline(source_folder='data')
