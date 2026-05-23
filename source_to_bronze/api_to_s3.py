import boto3
import requests
import json
from datetime import datetime
# pyrefly: ignore [missing-import]
from config.constants import AWS_REGION, STREAMS, API_ENDPOINTS, S3_BUCKET, S3_PREFIX

kinesis = boto3.client("kinesis", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# 🔹 Create stream if not exists
def create_stream(stream_name):
    try:
        kinesis.create_stream(
            StreamName=stream_name,
            ShardCount=1
        )
        print(f"Creating stream: {stream_name}")

        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=stream_name)

        print(f"{stream_name} ACTIVE ✅")

    except kinesis.exceptions.ResourceInUseException:
        print(f"{stream_name} already exists ✅")


# 🔹 Fetch API data
def fetch_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


# 🔹 Send events to Kinesis
def send_to_kinesis(stream_name, data, entity):
    for record in data:
        partition_key = str(
            record.get(f"{entity[:-1]}_id", record.get("id", "1"))
        )

        kinesis.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )

    print(f"Sent {len(data)} {entity} records to Kinesis 🚀")


# 🔹 Save to S3 Bronze (partitioned)
def save_to_s3(entity, data):
    now = datetime.utcnow()

    partition_path = (
        f"{S3_PREFIX}/{entity}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    )

    file_name = f"{entity}_{now.strftime('%Y%m%d_%H%M%S')}.json"
    s3_key = partition_path + file_name

    # newline JSON format (better for Athena later)
    body = "\n".join(json.dumps(record) for record in data)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body
    )

    print(f"Saved {entity} to s3://{S3_BUCKET}/{s3_key} ✅")


# 🔹 Main pipeline for each entity
def process_entity(entity):
    print(f"\nProcessing {entity}...")

    stream_name = STREAMS[entity]
    api_url = API_ENDPOINTS[entity]

    create_stream(stream_name)

    data = fetch_data(api_url)

    if not data:
        print(f"No data for {entity}")
        return

    send_to_kinesis(stream_name, data, entity)
    save_to_s3(entity, data)


# 🔹 Run for both
def main():
    for entity in ["payments", "campaigns"]:
        process_entity(entity)


if __name__ == "__main__":
    main()