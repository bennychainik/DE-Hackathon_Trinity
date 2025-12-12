import zipfile
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
    def extract_zip(zip_path: str, extract_to: str) -> list:
        """Extracts a zip file and returns list of extracted files."""
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
                extracted_files = [os.path.join(extract_to, f) for f in zip_ref.namelist()]
            logger.info(f"Extracted {zip_path} to {extract_to}")
        except Exception as e:
            logger.error(f"Failed to unzip {zip_path}: {e}")
        return extracted_files

    @staticmethod
    def read_folder(folder_path: str, file_pattern: str = "*", recursive: bool = False, **kwargs) -> pd.DataFrame:
        """
        Reads files from a folder. 
        Auto-handles ZIP extraction if zip files are found.
        """
        # 1. Handle ZIPs first
        zip_files = glob.glob(os.path.join(folder_path, "*.zip"))
        if zip_files:
            logger.info(f"Found {len(zip_files)} zip files. Extracting...")
            for zf in zip_files:
                FileIngestor.extract_zip(zf, folder_path)
        
        # 2. Read contents (CSV/Excel)
        # Supports CSV and XLSX
        search_path = os.path.join(folder_path, file_pattern)
        all_files = glob.glob(search_path, recursive=recursive)
        
        df_list = []
        for file in all_files:
            # Skip the zip files themselves and temp files
            if file.endswith('.zip') or file.startswith('~$'):
                continue
                
            try:
                if file.endswith('.csv'):
                    df = pd.read_csv(file, **kwargs)
                elif file.endswith('.xlsx') or file.endswith('.xls'):
                    df = pd.read_excel(file, **kwargs)
                else:
                    continue
                
                df['source_file'] = os.path.basename(file)
                df_list.append(df)
            except Exception as e:
                logger.error(f"Failed to read {file}: {e}")
        
        if df_list:
            final_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"Ingested {len(df_list)} files from {folder_path}. Total Shape: {final_df.shape}")
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
