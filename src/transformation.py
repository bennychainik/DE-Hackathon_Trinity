import pandas as pd
import numpy as np
from datetime import datetime
from src.utils import setup_logger

logger = setup_logger('transformation')

class Transformer:
    @staticmethod
    def join_tables(left_df: pd.DataFrame, right_df: pd.DataFrame, 
                    left_on: str, right_on: str, how: str = 'left') -> pd.DataFrame:
        """Generic join wrapper."""
        try:
            res = pd.merge(left_df, right_df, left_on=left_on, right_on=right_on, how=how)
            logger.info(f"Joined tables: {len(left_df)} rows + {len(right_df)} rows -> {len(res)} rows.")
            return res
        except Exception as e:
            logger.error(f"Join failed: {e}")
            raise e

    @staticmethod
    def scd_type_1(source_df: pd.DataFrame, target_df: pd.DataFrame, key_col: str) -> pd.DataFrame:
        """
        Implements SCD Type 1 (Overwrite).
        Updates target_df with source_df values for matching keys.
        Adds new rows from source_df.
        """
        # Set index for easy update
        target_df = target_df.set_index(key_col)
        source_df = source_df.set_index(key_col)
        
        # Update existing
        target_df.update(source_df)
        
        # Add new
        new_rows = source_df[~source_df.index.isin(target_df.index)]
        final_df = pd.concat([target_df, new_rows]).reset_index()
        
        logger.info(f"SCD Type 1 Complete. Total rows: {len(final_df)}")
        return final_df

    @staticmethod
    def scd_type_2(new_df: pd.DataFrame, existing_df: pd.DataFrame, 
                   join_keys: list, compare_cols: list) -> tuple:
        """
        Implements SCD Type 2 Logic.
        Returns two dataframes:
        1. records_to_insert (New IDs + New Versions of changed IDs)
        2. records_to_update (Old Versions to expire)
        
        Assumes existing_df has 'customer_sk' (or equivalent), 'eff_start_dt', 'eff_end_dt', 'current_flag'.
        new_df should have business keys + compare columns.
        """
        try:
            # 1. Identify New Records (Business Key not in Existing)
            # We assume join_keys is usually ['customer_id']
            # existing_df should be filtered to only current records (current_flag=1) before passing here usually, 
            # or we filter here. Let's assume passed existing_df is ALL active records.
            
            # Merge to find matches
            merged = pd.merge(
                new_df, 
                existing_df, 
                on=join_keys, 
                how='left', 
                suffixes=('', '_old'),
                indicator=True
            )
            
            # A. New Inserts (New Business Keys)
            new_inserts = merged[merged['_merge'] == 'left_only'][new_df.columns]
            
            new_inserts['current_flag'] = 1
            new_inserts['eff_start_dt'] = datetime.now().date()
            new_inserts['eff_end_dt'] = '9999-12-31'
            
            # B. Potential Updates (Keys exist, check if content changed)
            existing_matches = merged[merged['_merge'] == 'both']
            
            # Check for changes in compare_cols
            # We iterate or use vectorization. Vectorization is tricky with NaNs.
            # Simplified: Create a hash or string concat for comparison
            def create_hash(df, cols):
                return df[cols].astype(str).sum(axis=1)

            existing_matches['new_hash'] = create_hash(existing_matches, compare_cols)
            existing_matches['old_hash'] = create_hash(existing_matches, [c + '_old' for c in compare_cols])
            
            changed_rows = existing_matches[existing_matches['new_hash'] != existing_matches['old_hash']].copy()
            
            # For changed rows:
            # 1. We need to expire the OLD row (Update logic)
            # The df should carry the unique key (SK) of the old row to update it.
            # existing_df must provide 'customer_sk' (or similar PK).
            if 'customer_sk' in existing_df.columns:
                sk_col = 'customer_sk'
            else:
                # Fallback or error if we can't identify row to expire
                logger.warning("SCD2: No SK column found in existing data to close out.")
                sk_col = None

            updates = pd.DataFrame()
            if not changed_rows.empty and sk_col:
                updates = changed_rows[[sk_col]].copy()
                updates['current_flag'] = 0
                updates['eff_end_dt'] = datetime.now().date()
                
            # 2. We need to insert the NEW version
            # Use columns from new_df
            inserts_from_updates = changed_rows[new_df.columns].copy()
            inserts_from_updates['current_flag'] = 1
            inserts_from_updates['eff_start_dt'] = datetime.now().date()
            inserts_from_updates['eff_end_dt'] = '9999-12-31'
            
            # Combine all inserts
            all_inserts = pd.concat([new_inserts, inserts_from_updates])
            
            logger.info(f"SCD2: Found {len(new_inserts)} new, {len(changed_rows)} changed rows.")
            
            return all_inserts, updates

        except Exception as e:
            logger.error(f"SCD Type 2 Logic Failed: {e}")
            raise e 

    @staticmethod
    def aggregations(df: pd.DataFrame, group_cols: list, agg_dict: dict) -> pd.DataFrame:
        """
        Generic aggregation function.
        agg_dict example: {'salary': 'sum', 'id': 'count'}
        """
        return df.groupby(group_cols).agg(agg_dict).reset_index()

    @staticmethod
    def split_dataframe(df: pd.DataFrame, columns: list, drop_duplicates: bool = True) -> pd.DataFrame:
        """
        Safely selects columns from DataFrame if they exist.
        Useful for splitting massive flat files into Dims/Facts.
        """
        existing_cols = [c for c in columns if c in df.columns]
        missing_cols = list(set(columns) - set(existing_cols))
        
        if missing_cols:
            logger.warning(f"Columns missing for split: {missing_cols}")
        
        subset = df[existing_cols]
        if drop_duplicates:
            return subset.drop_duplicates()
        return subset

    @staticmethod
    def calculate_late_fees(df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates Late Fees based on user rules:
        Duration = Actual_Paid - Next_Due
        Rules: 5 months -> 2.5%, 6 months -> 3%
        """
        try:
            req_cols = ['actual_premium_paid_dt', 'next_premium_dt', 'premium_amt']
            if not all(col in df.columns for col in req_cols):
                logger.warning(f"Missing columns for Late Fee Calc. Need: {req_cols}")
                return df

            # 1. Calc Duration (Days -> Months)
            # User guideline: 178 days approx 6 months. Using /30 and rounding.
            df['late_duration_days'] = (df['actual_premium_paid_dt'] - df['next_premium_dt']).dt.days
            df['late_duration_months'] = (df['late_duration_days'] / 30).round().fillna(0).astype(int)

            # 2. Determine %
            # Generalized Rule: 0.5% (0.005) penalty per month delayed.
            # (Fits user examples: 5mo -> 2.5%, 6mo -> 3%)
            def get_penalty(months):
                if months <= 0: return 0.0
                return months * 0.005

            df['late_fee_pct'] = df['late_duration_months'].apply(get_penalty)

            # 3. Calc Amounts
            df['late_fee_amount'] = df['premium_amt'] * df['late_fee_pct']
            df['total_amount_to_pay'] = df['premium_amt'] + df['late_fee_amount']

            logger.info("Late Fee Calculation Applied.")
            return df
        except Exception as e:
            logger.error(f"Error in Late Fee Calc: {e}")
            return df

    @staticmethod
    def construct_customer_name(df: pd.DataFrame) -> pd.DataFrame:
        """
        Combines Title, First, Middle, Last into 'customer_name'.
        Handles missing values safely.
        """
        try:
            name_cols = ['customer_title', 'customer_first_name', 'customer_middle_name', 'customer_last_name']
            # fillna only on existing cols
            existing_cols = [c for c in name_cols if c in df.columns]
            
            if not existing_cols:
                return df
                
            for c in existing_cols:
                df[c] = df[c].fillna('').astype(str).replace('nan', '')
            
            # Combine logic (only if we have at least first/last usually, but we combine all available)
            # Create a localized series
            full_name = df[existing_cols[0]] if existing_cols else pd.Series([''] * len(df))
            for col in existing_cols[1:]:
                full_name = full_name + " " + df[col]
            
            # Clean spaces
            # Clean spaces
            calculated_name = full_name.str.replace(r'\s+', ' ', regex=True).str.strip().str.title()
            
            # If customer_name already exists (from other files), fill only missing
            if 'customer_name' in df.columns:
                df['customer_name'] = df['customer_name'].fillna(calculated_name)
            else:
                df['customer_name'] = calculated_name
                
            return df
        except Exception as e:
            logger.error(f"Error constructing customer name: {e}")
            return df
