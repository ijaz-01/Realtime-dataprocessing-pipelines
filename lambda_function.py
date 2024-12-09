import json
import boto3
import base64
from datetime import datetime

# Initialize the S3 client
s3_client = boto3.client('s3')

# S3 bucket and folder for storing transformed data
S3_BUCKET_NAME = 'ijazbucket'
S3_TRANSFORMED_FOLDER = 'transformed-data/'

def lambda_handler(event, context):
    transformed_records = []
    
    for record in event['Records']:
        # Decode the Kinesis data (Base64 encoded)
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        
        try:
            # Parse JSON data from the stream
            data = json.loads(payload)
            
            # Example transformation: Add a timestamp and filter out invalid data
            if "value" in data and data["value"] > 0:  # Only include positive values
                data["processed_timestamp"] = datetime.utcnow().isoformat()
                
                # Example enrichment: Adding a geolocation placeholder
                data["geo_location"] = "SampleLocation"
                
                # Append the transformed record
                transformed_records.append(data)
        
        except json.JSONDecodeError:
            print(f"Error decoding JSON: {payload}")
            continue
    
    # Save the transformed data to S3
    if transformed_records:
        save_to_s3(transformed_records)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed records.')
    }

def save_to_s3(records):
    """
    Save transformed records to an S3 bucket in JSON format.
    """
    # Generate a file name with a timestamp
    file_name = f"{S3_TRANSFORMED_FOLDER}{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    
    # Convert the records to JSON format
    file_content = json.dumps(records)
    
    # Upload the file to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_name,
        Body=file_content
    )
    print(f"Uploaded transformed data to S3: {file_name}")
