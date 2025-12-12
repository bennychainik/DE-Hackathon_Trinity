import logging
import os
import yaml
from pathlib import Path
from sqlalchemy import create_engine

# --- Logging Setup ---
def setup_logger(name: str, log_file: str = 'logs/pipeline.log', level=logging.INFO):
    """Function to setup as many loggers as you want"""
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger('etl_utils')

# --- Configuration ---
def load_config(config_path: str = 'config.yaml'):
    """Load configuration from yaml file."""
    if not os.path.exists(config_path):
        logger.warning(f"Config file {config_path} not found.")
        return {}
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# --- Database Connection ---
def get_db_connection(db_type='mysql', config=None):
    """
    Factory to get database connection engine.
    Prerequisites:
      pip install mysql-connector-python sqlalchemy
    """
    if config is None:
        # Fallback to env vars or default for hackathon speed
        user = os.getenv('DB_USER', 'root')
        password = os.getenv('DB_PASSWORD', 'password')
        host = os.getenv('DB_HOST', 'localhost')
        port = os.getenv('DB_PORT', '3306')
        db_name = os.getenv('DB_NAME', 'hackathon_db')
    else:
        user = config.get('user')
        password = config.get('password')
        host = config.get('host')
        port = config.get('port')
        db_name = config.get('db_name')

    if db_type == 'mysql':
        # Connection string: mysql+mysqlconnector://user:password@host:port/dbname
        conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db_name}"
        try:
            engine = create_engine(conn_str)
            logger.info(f"Connected to MySQL database: {db_name} at {host}")
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise e
    elif db_type == 'sqlite':
        return create_engine(f'sqlite:///{db_name}.db')
    else:
        raise ValueError(f"Unsupported DB type: {db_type}")
