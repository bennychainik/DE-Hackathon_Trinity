import pandas as pd
import glob
import os
from src.utils import setup_logger, get_db_connection

logger = setup_logger('ingestion')

class Ingestor:
    """Base Ingestion Class"""
    def __init__(self):
        pass

class FileIngestor(Ingestor):
    @staticmethod
    def read_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """Reads a single CSV file."""
        try:
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Successfully read CSV: {file_path}. Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV {file_path}: {e}")
            raise e

    @staticmethod
    def read_excel(file_path: str, sheet_name=0, **kwargs) -> pd.DataFrame:
        """Reads an Excel file."""
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Successfully read Excel: {file_path}. Shape: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"Error reading Excel {file_path}: {e}")
            raise e

    @staticmethod
    def read_folder(folder_path: str, file_pattern: str = "*.csv", **kwargs) -> pd.DataFrame:
        """Reads multiple files from a folder and concatenates them."""
        all_files = glob.glob(os.path.join(folder_path, file_pattern))
        if not all_files:
            logger.warning(f"No files found in {folder_path} with pattern {file_pattern}")
            return pd.DataFrame()
        
        df_list = []
        for file in all_files:
            try:
                # Simple logic to distinguish csv/excel based on extension if needed, 
                # but assumes pattern matches read function.
                if file.endswith('.csv'):
                    df = pd.read_csv(file, **kwargs)
                elif file.endswith('.xlsx'):
                    df = pd.read_excel(file, **kwargs)
                else:
                    logger.warning(f"Unsupported file type for auto-read: {file}")
                    continue
                df['source_file'] = os.path.basename(file) # Track source
                df_list.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file}: {e}")
        
        if df_list:
            final_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"Ingested {len(all_files)} files from {folder_path}. Total Shape: {final_df.shape}")
            return final_df
        else:
            return pd.DataFrame()

class SQLIngestor(Ingestor):
    def __init__(self, db_config=None, db_type='mysql'):
        self.engine = get_db_connection(db_type, db_config)

    def read_query(self, query: str) -> pd.DataFrame:
        """Executes a SQL query and returns a DataFrame."""
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Executed query. Returned {len(df)} rows.")
            return df
        except Exception as e:
            logger.error(f"Error executing query: {query}\nError: {e}")
            raise e

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Reads an entire table."""
        return self.read_query(f"SELECT * FROM {table_name}")
