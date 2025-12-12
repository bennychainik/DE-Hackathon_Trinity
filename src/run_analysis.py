import sys
import os
import pandas as pd
import re

# Add project root to path
sys.path.append(os.getcwd())

from src.utils import get_db_connection

def run_analysis():
    # Set Config explicitly to avoid default fallback failure
    os.environ['DB_USER'] = 'root'
    os.environ['DB_PASSWORD'] = '5500@Jesus'
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '3306'
    os.environ['DB_NAME'] = 'insurance_dw'

    print("--- Running Analysis Queries ---")
    
    # 1. Read SQL File
    sql_path = os.path.join('sql', 'reporting', 'analysis_queries.sql')
    if not os.path.exists(sql_path):
        print(f"Error: File not found {sql_path}")
        return

    with open(sql_path, 'r') as f:
        content = f.read()

    # 2. Split Queries
    # Logic: Split by semicolon, filter out empty or comment-only blocks
    # Note: This is a simple parser. It might break on semicolons inside strings.
    # Given the file content, split by ';' is safe enough.
    queries = content.split(';')
    
    engine = get_db_connection('mysql')

    query_count = 0
    for q in queries:
        q_trim = q.strip()
        if not q_trim:
            continue
            
        # Extract comment description (lines starting with --)
        lines = q_trim.split('\n')
        comment_lines = [l for l in lines if l.strip().startswith('--')]
        code_lines = [l for l in lines if not l.strip().startswith('--')]
        
        description = "\n".join(comment_lines)
        sql_code = "\n".join(code_lines).strip()
        
        if not sql_code:
            continue
            
        query_count += 1
        print(f"\n=========================================================")
        print(f"QUERY #{query_count}")
        print(f"Description:\n{description}")
        print(f"---------------------------------------------------------")
        print(f"SQL Repr:\n{repr(sql_code)}")
        
        try:
            df = pd.read_sql(sql_code, engine)
            if df.empty:
                print("Result: [Empty DataFrame]")
            else:
                print(f"Result: {len(df)} rows")
                print(df.head(10).to_string(index=False))
        except Exception as e:
            print(f"Execution Failed: {e}")

if __name__ == "__main__":
    run_analysis()
