import os
import boto3
import pandas as pd
# pyrefly: ignore [missing-import]
# pyrefly: ignore [missing-import]
from sqlalchemy import create_engine, text
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
from config.db_config import get_db_engine
from datetime import datetime
import uuid

# Load environment variables (from .env file)
load_dotenv()

# AWS Configuration
# Add these variables to your .env file!
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "customer-lifecycle-intelligence-platform-bronze-layer")

# Database Configuration 
# Note: Using 127.0.0.1 (localhost) to connect from the Mac terminal to the Docker MySQL container
DB_HOST = os.getenv("DB_HOST", "127.0.0.1") 
DB_PORT = os.getenv("DB_PORT", "3333")  # Update to 3306 if using the edtech_db container
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "edtech")

def export_table_to_s3(table_name):
    """
    Extracts data from the given MySQL table and uploads it to AWS S3.
    """
    print(f"Extracting data from '{table_name}' table...")
    
    # 1. Connect to MySQL Database
    engine = get_db_engine()
    
    # 2. Check for existing watermark
    watermark_query = text("SELECT last_watermark FROM etl_control WHERE table_name = :table AND is_latest = TRUE")
    with engine.connect() as conn:
        result = conn.execute(watermark_query, {"table": table_name}).fetchone()
        last_watermark = result[0] if result else None

    # 3. Build the Extraction Query with a 2-HOUR LOOKBACK BUFFER
    if last_watermark:
        print(f"🔄 Found watermark: {last_watermark}. Applying 2-hour lookback buffer.")
        # We reach back 2 hours to capture any late-arriving or delayed records
        query = f"SELECT * FROM {table_name} WHERE updated_at >= DATE_SUB('{last_watermark}', INTERVAL 2 HOUR)"
    else:
        print(f"⏳ No watermark found for {table_name}. Running FULL load.")
        query = f"SELECT * FROM {table_name}"
        
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        # Fallback in case a table (like an associative table) is missing updated_at
        print(f"⚠️ Error running incremental query (possibly missing updated_at column). Running full load: {e}")
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    
    if df.empty:
        print(f"⚠️ No new records found in {table_name} since last watermark. Skipping.")
        return
        
    # 4. Find the new highest watermark from the extracted data
    new_watermark = None
    if 'updated_at' in df.columns:
        new_watermark = df['updated_at'].max()
    elif 'created_at' in df.columns:
        new_watermark = df['created_at'].max()
        
    print(f"✅ Extracted {len(df)} rows from {table_name}.")
    
    # 3. Save Data locally (as CSV, Parquet is also a great option for Bronze)
    # Using /tmp directory which works smoothly on Mac/Linux
    local_file_path = f"/tmp/{table_name}_bronze_raw.csv"
    df.to_csv(local_file_path, index=False)
    
    # 4. Upload to S3
    # Note: boto3 automatically looks for AWS credentials in ~/.aws/credentials if not provided explicitly here
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    # Generate the run_id (batch_id) for this extraction
    run_id = str(uuid.uuid4())
    
    # Structure S3 folders using Hive-style partitioning: bronze/<table_name>/year=YYYY/month=MM/day=DD/run_id=UUID/<filename>
    now = datetime.now()
    partition = f"year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/run_id={run_id}"
    s3_key = f"bronze/{table_name}/{partition}/{table_name}_data.csv"
    
    print(f"⬆️ Uploading to s3://{S3_BUCKET_NAME}/{s3_key}...")
    try:
        s3_client.upload_file(local_file_path, S3_BUCKET_NAME, s3_key)
        print(f"🎉 Successfully uploaded {table_name} to S3!")
        
        # 5. Update the Control Table with the new watermark!
        if new_watermark:
            now_ts = datetime.now()
            with engine.begin() as conn:
                # Deprecate the old watermark
                conn.execute(text("UPDATE etl_control SET is_latest = FALSE WHERE table_name = :table"), {"table": table_name})
                # Insert the new watermark
                conn.execute(text("""
                    INSERT INTO etl_control (table_name, last_watermark, is_latest, run_id, run_timestamp)
                    VALUES (:table, :wm, TRUE, :run_id, :now_ts)
                """), {"table": table_name, "wm": new_watermark, "run_id": run_id, "now_ts": now_ts})
            print(f"📝 Updated control table watermark to: {new_watermark}")
            
    except Exception as e:
        print(f"❌ Error uploading to S3 or updating control table: {e}")
    finally:
        # 5. Clean up local temporary file
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

if __name__ == "__main__":
    # Define which tables you want to push to the Bronze layer
    tables_to_export = [
        "session_attendance", 
        "calls",
        "lead_status_history",
        "activities",
        "opportunities",
        "refunds", 
        "sessions", 
        "counselors",
        "users",
        "etl_control"
    ]
    
    for table in tables_to_export:
        export_table_to_s3(table)
        print("-" * 50)
