import pandas as pd
from src.utils import setup_logger

logger = setup_logger('validation')

class Validator:
    @staticmethod
    def check_nulls(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
        """Checks for null values. If columns is None, checks all."""
        cols_to_check = columns if columns else df.columns
        null_report = df[cols_to_check].isnull().sum()
        
        if null_report.sum() > 0:
            logger.warning(f"Nulls detected:\n{null_report[null_report > 0]}")
        else:
            logger.info("No nulls detected.")
        return null_report

    @staticmethod
    def check_duplicates(df: pd.DataFrame, subset: list = None) -> int:
        """Checks for duplicate rows."""
        dupes = df.duplicated(subset=subset).sum()
        if dupes > 0:
            logger.warning(f"Found {dupes} duplicate rows.")
        else:
            logger.info("No duplicates found.")
        return dupes

    @staticmethod
    def check_referential_integrity(fact_df: pd.DataFrame, dim_df: pd.DataFrame, 
                                    fact_key: str, dim_key: str) -> pd.DataFrame:
        """
        Checks if keys in fact table exist in dimension table.
        Returns: DataFrame of orphaned records (facts with no matching dim).
        """
        orphans = fact_df[~fact_df[fact_key].isin(dim_df[dim_key])]
        if not orphans.empty:
            logger.warning(f"Referential Integrity Failed: {len(orphans)} rows in fact table have no match in dimension.")
        else:
            logger.info("Referential Integrity Check Passed.")
        return orphans

    @staticmethod
    def validation_report(df: pd.DataFrame) -> dict:
        """Generates a generic profile report."""
        report = {
            'rows': len(df),
            'columns': len(df.columns),
            'nulls_total': df.isnull().sum().sum(),
            'duplicates': df.duplicated().sum(),
            'dtypes': df.dtypes.to_dict()
        }
        logger.info(f"Validation Report: {report}")
        return report
