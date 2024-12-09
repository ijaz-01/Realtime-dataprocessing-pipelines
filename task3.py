import boto3
import json
import random
import time

import boto3

aws_access_key_id = 'AKIAUZPNLJFEBNOFJP4W'
aws_secret_access_key = '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'

kinesis_client = boto3.client(
    'kinesis',
    aws_access_key_id="AKIAUZPNLJFEBNOFJP4W",
    aws_secret_access_key="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",
    region_name='eu-north-1'
)


STREAM_NAME = "ijaz"

def get_sensor_data():
    return {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": time.time()
    }

while True:
    data = get_sensor_data()
    print(f"Sending data: {data}")
    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=str(data['sensor_id'])
    )
time.sleep(10)