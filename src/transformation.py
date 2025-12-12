import pandas as pd
import numpy as np
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
    def scd_type_2(source_df: pd.DataFrame, target_df: pd.DataFrame, 
                   key_col: str, effective_date: str) -> pd.DataFrame:
        """
        SCD Type 2 (History Retention) boilerplate.
        Needs specific column inputs for 'valid_from', 'valid_to', 'is_current'.
        """
        # This is a complex logic usually best handled by specific requirements,
        # but here is a template concept:
        # 1. Identify changed records
        # 2. Close out old records (update valid_to, is_current=False)
        # 3. Insert new records (valid_from=today, valid_to=NULL, is_current=True)
        # This function returns a placeholder logic string or basic merging for now.
        logger.info("SCD Type 2 Template applied (Logic requires customization).")
        return pd.DataFrame() 

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
