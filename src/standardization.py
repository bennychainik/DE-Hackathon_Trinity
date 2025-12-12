import pandas as pd
import re
from src.utils import setup_logger

logger = setup_logger('standardization')

class Standardizer:
    @staticmethod
    def to_snake_case(column_name: str) -> str:
        """Converts string to snake_case."""
        # Replace non-alphanumeric with underscore and lower
        s = re.sub(r'[^a-zA-Z0-9]', '_', column_name.strip())
        s = re.sub(r'_+', '_', s) # Remove aggregate underscores
        return s.lower().strip('_')

    @staticmethod
    def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Renames all columns to snake_case."""
        new_cols = {col: Standardizer.to_snake_case(col) for col in df.columns}
        df = df.rename(columns=new_cols)
        
        # Remove duplicate columns if any (keeping first)
        df = df.loc[:, ~df.columns.duplicated()]
        
        logger.info(f"Standardized column names. Shape: {df.shape}")
        return df

    @staticmethod
    def clean_currency(df: pd.DataFrame, cols: list) -> pd.DataFrame:
        """Removes '$', ',' from currency columns and converts to float."""
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'[$,]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    @staticmethod
    def parse_dates(df: pd.DataFrame, cols: list, format: str = None) -> pd.DataFrame:
        """Converts columns to datetime."""
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=format, errors='coerce')
        return df

    @staticmethod
    def trim_strings(df: pd.DataFrame) -> pd.DataFrame:
        """Trims whitespace from all string columns."""
        str_cols = df.select_dtypes(include=['object']).columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
        return df

    @staticmethod
    def clean_country(df: pd.DataFrame, col: str = 'country') -> pd.DataFrame:
        """
        Standardizes country names.
        Maps 'usa', 'u.s.', 'united states' -> 'USA'.
        """
        if col in df.columns:
            # Normalize to lowercase for comparison
            s = df[col].astype(str).str.lower().str.strip()
            
            # Map USA variations
            # Regex could be used, but simple map/replace is often faster/safer for known set
            # 'usa', 'u.s.', 'u.s', 'united states', 'united states of america'
            usa_variants = ['usa', 'u.s.', 'u.s', 'united states', 'united states of america', 'us']
            
            # Application
            df.loc[s.isin(usa_variants), col] = 'USA'
            
            logger.info(f"Standardized '{col}' column for USA variations.")
        return df
