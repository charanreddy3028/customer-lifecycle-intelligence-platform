import json
import boto3
import urllib.parse
import os
import pandas as pd
# Note: Requires AWS Data Wrangler (awswrangler) Lambda Layer to write Parquet easily!
# pyrefly: ignore [missing-import]
import awswrangler as wr

s3_client = boto3.client('s3')
silver_bucket = os.environ['SILVER_BUCKET']

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    
    # Process every record (S3 object) that triggered this lambda
    for record in event['Records']:
        bronze_bucket = record['s3']['bucket']['name']
        bronze_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        print(f"Processing s3://{bronze_bucket}/{bronze_key}")
        
        # 1. Determine entity type based on folder path (e.g., bronze/payments/...)
        path_parts = bronze_key.split('/')
        if len(path_parts) < 2:
            continue
        entity = path_parts[1]
        
        if entity not in ['payments', 'campaigns']:
            print(f"Ignoring non-API entity: {entity}")
            continue

        # 2. Read the raw JSON data from Bronze
        response = s3_client.get_object(Bucket=bronze_bucket, Key=bronze_key)
        raw_content = response['Body'].read().decode('utf-8')
        
        # The API script dumps newline-delimited JSON
        json_objects = [json.loads(line) for line in raw_content.strip().split('\n') if line]
        
        if not json_objects:
            print("Empty file, skipping.")
            continue
            
        # 3. Transform & Cleanse Data using Pandas
        df = pd.DataFrame(json_objects)
        
        # Deduplicate and Clean
        if entity == 'payments':
            if 'payment_mode' not in df.columns:
                df['payment_mode'] = 'UNKNOWN'
            if 'failure_reason' not in df.columns:
                df['failure_reason'] = None
            if 'currency' not in df.columns:
                df['currency'] = 'UNKNOWN'
                
            df['payment_mode'] = df['payment_mode'].fillna('UNKNOWN')
            df['currency'] = df['currency'].fillna('UNKNOWN')
            
            # Deduplicate by payment_id keeping the latest payment record
            if 'payment_id' in df.columns and 'payment_datetime' in df.columns:
                df = df.sort_values(by='payment_datetime', ascending=False)
                df = df.drop_duplicates(subset=['payment_id'], keep='first')

        elif entity == 'campaigns':
            # Deduplicate by campaign_id
            if 'campaign_id' in df.columns:
                sort_col = 'updated_at' if 'updated_at' in df.columns else 'start_date'
                if sort_col in df.columns:
                    df = df.sort_values(by=sort_col, ascending=False)
                df = df.drop_duplicates(subset=['campaign_id'], keep='first')
            
        # Add Standard Metadata Columns
        df['ingested_datetime'] = pd.Timestamp.now(tz='Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
        df['source'] = f"api_{entity}"
        # Use the S3 filename as the batch_id to enable clear lineage back to the raw JSON file
        df['batch_id'] = os.path.basename(bronze_key)
            
        print(f"Transformed and deduplicated {len(df)} records for {entity}.")
        
        # 4. Write to Silver in Parquet format using awswrangler
        # Maintain the same partitioning structure
        silver_key = bronze_key.replace("bronze/", "silver/").replace(".json", ".parquet")
        silver_path = f"s3://{silver_bucket}/{silver_key}"
        
        print(f"Writing to {silver_path}...")
        
        wr.s3.to_parquet(
            df=df,
            path=silver_path,
            dataset=False
        )
        
        print(f"✅ Successfully processed and moved {entity} to Silver!")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }
