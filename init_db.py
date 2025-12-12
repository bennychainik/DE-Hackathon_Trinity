from sqlalchemy import create_engine, text
import os

# Connect to Server (no DB)
user = os.getenv('DB_USER', 'root')
password = os.getenv('DB_PASSWORD', 'password')
from urllib.parse import quote_plus
host = os.getenv('DB_HOST', 'localhost')
port = os.getenv('DB_PORT', '3306')

encoded_password = quote_plus(password)
conn_str = f"mysql+mysqlconnector://{user}:{encoded_password}@{host}:{port}"
engine = create_engine(conn_str, isolation_level="AUTOCOMMIT")

print("Initializing Database...")
try:
    with engine.connect() as conn:
        with open('sql/ddl/staging_and_dwh.sql', 'r') as f:
            sql_script = f.read()
        
        # Split by ; to run statements
        statements = sql_script.split(';')
        for stmt in statements:
            if stmt.strip():
                try:
                    conn.execute(text(stmt))
                    print(f"Executed: {stmt[:50]}...")
                except Exception as e:
                    print(f"Error executing statement: {e}")
                    
    print("Database Initialized Successfully.")
except Exception as e:
    print(f"Failed to initialize: {e}")
