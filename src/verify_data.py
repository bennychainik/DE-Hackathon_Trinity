import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Database Connection
user = os.getenv('DB_USER', 'root')
password = os.getenv('DB_PASSWORD', '5500@Jesus')
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '3306')
db_name = 'insurance_dw'

encoded_password = quote_plus(password)
conn_str = f"mysql+mysqlconnector://{user}:{encoded_password}@{host}:{port}/{db_name}"
engine = create_engine(conn_str)

def check_table(table_name):
    try:
        df = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", engine)
        count = df['count'].iloc[0]
        print(f"✅ {table_name}: {count} rows")
    except Exception as e:
        print(f"❌ {table_name}: Error - {e}")

print("--- Data Warehouse Verification ---")
tables = [
    'stg_customers', 'stg_policy', 'stg_transactions', 
    'dim_customer', 'dim_policy', 'fact_policy_txn'
]

for t in tables:
    check_table(t)
