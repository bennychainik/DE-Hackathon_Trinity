import sys
import os
import traceback
import glob
import re
from datetime import datetime
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from src.utils import setup_logger
from src.ingestion import FileIngestor, SQLIngestor
from src.validation import Validator
from src.standardization import Standardizer
from src.transformation import Transformer
from src.loader import Loader

logger = setup_logger('main_pipeline')

def process_batch(df: pd.DataFrame, batch_name: str = "Unknown"):
    """
    Processing logic for a single dataframe/batch.
    Contains Steps 2 (Standardization) through 7 (Loading).
    """
    logger.info(f"--- Processing Batch: {batch_name} (Rows: {len(df)}) ---")

    # 2. STANDARDIZATION (Column Names)
    # ---------------------------------------------------------
    logger.info("Step 2: Standardization")
    df = Standardizer.standardize_columns(df)
    
    # 3. VALIDATION
    # ---------------------------------------------------------
    logger.info("Step 3: Validation")
    if 'premium_amt' not in df.columns and 'premium_amount' not in df.columns:
        logger.warning("Critical columns missing (premium logic might fail).")

    # 4. TRANSFORMATION & MAPPING
    # ---------------------------------------------------------
    logger.info("Step 4: Transformation & Cleaning")

    # 4.1 Construct Customer Name
    df = Transformer.construct_customer_name(df)
    
    # 4.2 Rename/Map Columns
    rename_map = {
        'policy_type': 'policy_type_name', 
        'state_or_province': 'state_province',
        'postal_code': 'postal_code',
        'maritial_status': 'marital_status'
    }
    df = df.rename(columns=rename_map)

    # 4.2.1 Enforce String Types for IDs
    for col in ['policy_id', 'policy_type_id', 'customer_id']:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()

    # 4.3 Clean Strings
    df = Standardizer.trim_strings(df)
    
    # NEW: Standardize Country
    df = Standardizer.clean_country(df, col='country')
    
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
    df = Transformer.calculate_late_fees(df)

    # 5. PREPARE FOR STAGING
    # ---------------------------------------------------------
    df['ingestion_date'] = datetime.now()
    if 'source_file' not in df.columns:
        df['source_file'] = 'unknown'
        
    if 'actual_premium_paid_dt' in df.columns:
        df['txn_date'] = df['actual_premium_paid_dt']
    
    if 'premium_amt' in df.columns:
        df['premium_amount'] = df['premium_amt']
    
    if 'premium_amt_paid_tilldate' in df.columns:
        df['premium_paid_tilldate'] = df['premium_amt_paid_tilldate']


    # 6. LOADING (Staging Layer)
    # ---------------------------------------------------------
    logger.info("Step 6: Loading Staging Layer")
    loader = Loader(db_type='mysql')
    
    stg_cust = Transformer.split_dataframe(df, [
        'customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 
        'dob', 'effective_start_dt', 'effective_end_dt', 'region', 'ingestion_date', 'source_file'
    ])
    
    df['policy_type'] = df.get('policy_type_name') 
    
    stg_pol = Transformer.split_dataframe(df, [
        'policy_id', 'policy_name', 'policy_type_id', 'policy_type', 'policy_type_desc', 
        'policy_term', 'policy_start_dt', 'policy_end_dt', 'total_policy_amt', 'premium_amt', 
        'next_premium_dt', 'actual_premium_paid_dt', 'premium_amt_paid_tilldate', 
        'customer_id', 'region', 'ingestion_date', 'source_file'
    ])

    stg_addr = Transformer.split_dataframe(df, [
        'customer_id', 'country', 'region', 'state_province', 'city', 'postal_code', 
        'ingestion_date', 'source_file'
    ])
    
    stg_txn = Transformer.split_dataframe(df, [
        'policy_id', 'customer_id', 'txn_date', 'premium_amount', 'premium_paid_tilldate', 
        'total_policy_amt', 'region', 'ingestion_date', 'source_file'
    ])

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
    dim_policy = stg_pol[['policy_id', 'policy_name', 'policy_type_id', 'policy_type', 'policy_term', 'policy_start_dt', 'policy_end_dt', 'total_policy_amt']].drop_duplicates('policy_id')
    dim_policy['created_at'] = datetime.now()
    loader.load_to_db(dim_policy, 'dim_policy', if_exists='append')
    
    # 7.3 Dim Customer (SCD Type 2)
    dim_cust_new = stg_cust[['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'region', 'effective_start_dt']].drop_duplicates('customer_id')
    
    sql_reader = SQLIngestor(db_type='mysql')
    
    try:
        existing_cust = sql_reader.read_query("SELECT customer_sk, customer_id, customer_name, customer_segment, marital_status, region FROM dim_customer WHERE current_flag = 1")
    except Exception as e:
        logger.warning(f"Could not fetch existing DimCustomer (First Run?): {e}")
        existing_cust = pd.DataFrame()

    compare_cols = ['customer_name', 'customer_segment', 'marital_status', 'region']
    
    if existing_cust.empty:
        to_insert = dim_cust_new.copy()
        to_insert['current_flag'] = 1
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

    if not to_update.empty:
        logger.info(f"Expiring {len(to_update)} old customer records...")
        from sqlalchemy import text
        with loader.engine.connect() as conn:
            for index, row in to_update.iterrows():
                stmt = text(f"UPDATE dim_customer SET current_flag = 0, eff_end_dt = :end_dt WHERE customer_sk = :sk")
                conn.execute(stmt, {'end_dt': row['eff_end_dt'], 'sk': row['customer_sk']})
                conn.commit()

    if not to_insert.empty:
        dob_lookup = stg_cust[['customer_id', 'dob']].drop_duplicates('customer_id')
        to_insert = pd.merge(to_insert, dob_lookup, on='customer_id', how='left')
        to_insert['created_at'] = datetime.now()
        cols_to_load = ['customer_id', 'customer_name', 'customer_segment', 'marital_status', 'gender', 'dob', 'eff_start_dt', 'eff_end_dt', 'current_flag', 'region', 'created_at']
        for c in cols_to_load:
            if c not in to_insert.columns:
                to_insert[c] = None 
        loader.load_to_db(to_insert[cols_to_load], 'dim_customer', if_exists='append')

    # 7.4 Dim Address
    dim_addr = stg_addr[['customer_id', 'country', 'region', 'state_province', 'city', 'postal_code']].drop_duplicates(['customer_id', 'postal_code'])
    dim_addr['created_at'] = datetime.now()
    loader.load_to_db(dim_addr, 'dim_address', if_exists='append')
    
    # 7.5 Dim Late Fee
    fee_range = range(0, 61)
    dim_late_fee = pd.DataFrame({'duration_months': fee_range})
    dim_late_fee['penalty_percent'] = dim_late_fee['duration_months'] * 0.005
    dim_late_fee['description'] = dim_late_fee['duration_months'].astype(str) + " Months Delay"
    dim_late_fee['created_at'] = datetime.now()
    try:
        loader.load_to_db(dim_late_fee, 'dim_late_fee', if_exists='append') 
    except Exception:
        pass 

    # 7.6 Fact Policy Txn
    try:
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        # Optimized Fetch: Map Ids
        # We fetch minimal maps. Optimization: Cache maps if loops are fast? Safe to fetch fresh each batch.
        map_cust = sql_reader.read_query("SELECT customer_sk, customer_id, eff_start_dt, eff_end_dt FROM dim_customer")
        map_pol = sql_reader.read_query("SELECT policy_sk, policy_id FROM dim_policy")
        map_addr = sql_reader.read_query("SELECT address_sk, customer_id, postal_code FROM dim_address")
        map_fee = sql_reader.read_query("SELECT late_fee_sk, duration_months FROM dim_late_fee")
    except Exception as e:
        logger.error(f"Failed to fetch dimension maps for Fact linking: {e}")
        return

    # Map Cust
    map_cust['customer_id'] = map_cust['customer_id'].astype(str)
    map_pol['policy_id'] = map_pol['policy_id'].astype(str)
    map_addr['customer_id'] = map_addr['customer_id'].astype(str)
    if 'postal_code' in map_addr.columns:
        map_addr['postal_code'] = map_addr['postal_code'].astype(str)

    df_merged = pd.merge(df, map_cust, on='customer_id', how='left')
    df_merged['txn_date'] = pd.to_datetime(df_merged['txn_date'], errors='coerce')
    df_merged['eff_start_dt'] = pd.to_datetime(df_merged['eff_start_dt'], errors='coerce')
    df_merged['eff_end_dt'] = pd.to_datetime(df_merged['eff_end_dt'], errors='coerce')
    df_merged['eff_end_dt'] = df_merged['eff_end_dt'].fillna(pd.Timestamp.max)
    
    # Link to SCD2 Version
    mask = (df_merged['txn_date'] >= df_merged['eff_start_dt']) & (df_merged['txn_date'] <= df_merged['eff_end_dt'])
    fact_data = df_merged[mask].copy()

    # Map Policy
    if 'policy_id' in fact_data.columns:
        fact_data['policy_id'] = fact_data['policy_id'].astype(str)
        fact_data = pd.merge(fact_data, map_pol, on='policy_id', how='left')

    # Map Address
    if 'postal_code' in fact_data.columns:
        fact_data['postal_code'] = fact_data['postal_code'].fillna('').astype(str).replace(r'\.0$', '', regex=True)
        fact_data = pd.merge(fact_data, map_addr, on=['customer_id', 'postal_code'], how='left')
    elif 'customer_id' in fact_data.columns:
         fact_data = pd.merge(fact_data, map_addr, on='customer_id', how='left')

    # Map Late Fee
    if 'late_duration_months' in fact_data.columns:
        fact_data = pd.merge(fact_data, map_fee, left_on='late_duration_months', right_on='duration_months', how='left')

    # Date SK
    fact_data['date_sk'] = fact_data['txn_date'].dt.strftime('%Y%m%d').fillna(0).astype(int)

    final_cols = [
        'customer_sk', 'policy_sk', 'address_sk', 'late_fee_sk', 'date_sk', 
        'premium_amt', 'premium_paid_tilldate', 'total_policy_amt', 'late_fee', 'region', 'ingestion_date'
    ]
    
    if 'late_fee_amount' in fact_data.columns:
        fact_data = fact_data.rename(columns={'late_fee_amount': 'late_fee'})
    
    for c in final_cols:
        if c not in fact_data.columns:
            fact_data[c] = None

    loader.load_to_db(fact_data[final_cols], 'fact_policy_txn', if_exists='append')
    logger.info(f"Batch {batch_name} Load Complete.")


def run_pipeline(source_folder='data'):
    """
    Runs the ETL pipeline sequentially by Day (0, 1, 2...).
    """
    logger.info(f"Starting Sequential ETL Pipeline. Source: {source_folder}")
    
    # 1. Initialize Ingestor & Extract ZIPs
    ingestor = FileIngestor()
    
    # We trigger a read with a dummy pattern to force zip extraction (or assume extracted).
    # FileIngestor.read_folder extracts zips inside. 
    # To ensure all zips are extracted before we scan for "days", we run a quick pass or just scan zip names.
    # Logic: read_folder extracts ALL zips in valid folder.
    # Let's extracting by calling extract explicitly if possible, or just relying on glob if raw files exist.
    # Assuming the USER has zips. We'll find them using standard lib to be safe.
    
    # Let's manually trigger extraction using Ingestor's static method or internal logic? 
    # Helper: read_folder extracts zips if it finds them.
    # We can call read_folder with a pattern that matches nothing but triggers zip logic? 
    # Code review: read_folder globs *.zip, matches, extracts... THEN globs file_pattern.
    # So if we assume at least one file exists, we can run it. 
    # Or better: Just use glob in 'run_pipeline' to find zips and use Ingestor.extract_zip
    
    zip_files = glob.glob(os.path.join(source_folder, "*.zip"))
    for zf in zip_files:
        FileIngestor.extract_zip(zf, source_folder)

    # 2. Identify Unique Days from CSVs
    # Pattern: ..._dayX.csv
    # We search recursively or flat? 'data' usually flat or regional folders?
    # User showed: data/Insurance_details_US_Central_day0.csv. 
    # Flat structure in data/ after unzip.
    
    all_files = glob.glob(os.path.join(source_folder, "*_day*.csv"))
    
    days = set()
    for f in all_files:
        # Regex to find day number
        match = re.search(r'day(\d+)\.csv$', f, re.IGNORECASE)
        if match:
            days.add(int(match.group(1)))
            
    sorted_days = sorted(list(days))
    logger.info(f"Found Days to Process: {sorted_days}")
    
    if not sorted_days:
        logger.warning("No day-patterned files found. Running legacy mode (all files).")
        df = ingestor.read_folder(source_folder)
        process_batch(df, "Legacy-All")
        return

    # 3. Process Chronologically
    for day in sorted_days:
        pattern = f"*day{day}.csv"
        logger.info(f"=== Starting Batch: DAY {day} ===")
        
        # Read only files for this day
        # Ingestor.read_folder accepts pattern.
        df_batch = ingestor.read_folder(source_folder, file_pattern=pattern)
        
        if not df_batch.empty:
            process_batch(df_batch, batch_name=f"Day {day}")
        else:
            logger.warning(f"Batch Day {day} was empty.")
            
    logger.info("Sequential Pipeline Complete.")

if __name__ == "__main__":
    try:
        run_pipeline(source_folder='data')
    except Exception:
        traceback.print_exc()
