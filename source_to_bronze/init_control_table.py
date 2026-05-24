import sys
import os

# Ensure config module can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import get_db_engine
# pyrefly: ignore [missing-import]
from sqlalchemy import text

def create_control_table():
    engine = get_db_engine()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS etl_control (
        table_name VARCHAR(255) NOT NULL,
        last_watermark DATETIME NOT NULL,
        is_latest BOOLEAN DEFAULT TRUE,
        run_id VARCHAR(255) NOT NULL,
        run_timestamp DATETIME NOT NULL,
        PRIMARY KEY (table_name, run_id)
    );
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            print("✅ Successfully created `etl_control` table (or it already exists).")
    except Exception as e:
        print(f"❌ Failed to create control table: {e}")

if __name__ == "__main__":
    create_control_table()
