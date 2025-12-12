import sys
import os

# Ensure src is in path
sys.path.append(os.getcwd())

def verify_modules():
    print("Verifying Modules...")
    try:
        from src.utils import setup_logger, get_db_connection
        from src.ingestion import Ingestor
        from src.validation import Validator
        from src.standardization import Standardizer
        from src.transformation import Transformer
        print("✅ All Python Modules Imported Successfully")
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False
    return True

def verify_files():
    print("\nVerifying Important Files...")
    files = [
        'requirements.txt',
        'README.md',
        '.gitignore',
        'src/utils.py',
        'src/ingestion.py',
        'src/validation.py',
        'src/standardization.py',
        'src/transformation.py',
        'sql/ddl/init_schema_mysql.sql',
        'sql/reporting/analysis_queries.sql',
        'docs/team_roles.md'
    ]
    all_exist = True
    for f in files:
        if os.path.exists(f):
            print(f"✅ Found: {f}")
        else:
            print(f"❌ Missing: {f}")
            all_exist = False
    return all_exist

if __name__ == "__main__":
    if verify_modules() and verify_files():
        print("\n🚀 Project Setup Verified Successfully!")
    else:
        print("\n⚠️ Verification Failed. Check errors above.")
