import boto3
import requests
import json
# pyrefly: ignore [missing-import]
from config.constants import AWS_REGION, API_ENDPOINTS

firehose = boto3.client("firehose", region_name=AWS_REGION)

# 🔹 Fetch API data
def fetch_data(api_url):
    # Pass a large limit to generate/fetch all the data instead of the default 100
    response = requests.get(f"{api_url}?limit=1000")
    response.raise_for_status()
    return response.json()

# 🔹 Send events to Kinesis Data Firehose (Micro-batching)
def send_to_firehose(delivery_stream_name, data, entity):
    # AWS Firehose allows a maximum of 500 records per put_record_batch call
    BATCH_SIZE = 500
    
    records = []
    for record in data:
        # Firehose expects the data to be bytes. 
        # Adding a newline character ensures JSON lines format which is optimal for Athena/Glue.
        record_bytes = (json.dumps(record) + "\n").encode('utf-8')
        records.append({'Data': record_bytes})

    # Chunk the records and send to Firehose
    total_sent = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        response = firehose.put_record_batch(
            DeliveryStreamName=delivery_stream_name,
            Records=batch
        )
        # Check for failed records (optional but good practice)
        failed_put_count = response.get('FailedPutCount', 0)
        successful_puts = len(batch) - failed_put_count
        total_sent += successful_puts
        
        if failed_put_count > 0:
            print(f"⚠️ Failed to send {failed_put_count} records to {delivery_stream_name}")

    print(f"Sent {total_sent} {entity} records to Firehose Delivery Stream 🚀")

# 🔹 Main pipeline for each entity
def process_entity(entity):
    print(f"\nProcessing {entity}...")

    stream_name = f"{entity}-firehose-stream"
    api_url = API_ENDPOINTS[entity]

    data = fetch_data(api_url)

    if not data:
        print(f"No data for {entity}")
        return

    # We only need to push to Firehose now. Firehose handles S3 uploading automatically!
    send_to_firehose(stream_name, data, entity)

# 🔹 Run for both
def main():
    for entity in ["payments", "campaigns"]:
        process_entity(entity)

if __name__ == "__main__":
    main()