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

def preview_table(table_name, limit=5):
    print(f"\n{'='*50}")
    print(f"👀 PREVIEW: {table_name} (Top {limit})")
    print(f"{'='*50}")
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {limit}", engine)
        if df.empty:
            print("No data found.")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")

# Preview key tables
preview_table('dim_customer')
preview_table('fact_policy_txn')
