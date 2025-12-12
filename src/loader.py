import pandas as pd
from src.utils import setup_logger, get_db_connection

logger = setup_logger('loader')

class Loader:
    def __init__(self, db_type='mysql', config=None):
        """Initializes the Loader with a database connection."""
        self.engine = get_db_connection(db_type, config)

    def load_to_db(self, df: pd.DataFrame, table_name: str, if_exists='append', chunksize=1000):
        """
        Loads a pandas DataFrame into a database table.
        
        Args:
            df (pd.DataFrame): Data to load.
            table_name (str): Target table name in MySQL.
            if_exists (str): 'fail', 'replace', or 'append'. Default 'append'.
            chunksize (int): Rows to load per batch.
        """
        if df.empty:
            logger.warning(f"Dataframe for {table_name} is empty. Skipping load.")
            return

        try:
            df.to_sql(
                name=table_name, 
                con=self.engine, 
                if_exists=if_exists, 
                index=False, 
                chunksize=chunksize
            )
            logger.info(f"✅ Successfully loaded {len(df)} rows into table '{table_name}' (Mode: {if_exists}).")
        except Exception as e:
            logger.error(f"❌ Failed to load data to {table_name}: {e}")
            raise e
