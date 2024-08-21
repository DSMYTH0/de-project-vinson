import boto3
import json

def update_extracted_time(bucket_name, table, current_date, client=boto3.client('s3')):
    client.put_object(
    Body=json.dumps(str(current_date)),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )
    