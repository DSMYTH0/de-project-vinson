import boto3
import datetime
import json

def create_new_extract(bucket_name, table, client=boto3.client('s3')):
    client.put_object(
    Body=json.dumps(str(datetime.datetime(2000, 1, 1, 1, 1, 1, 0))),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )