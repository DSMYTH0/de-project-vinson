from src.extract.create_new_extract import create_new_extract
import boto3
import json

def get_last_extracted_time(bucket_name, table, client=boto3.client('s3')):
    """ 
        Function takes two arguments (e.g bucket name and a default client)
        Get response object from s3 bucket
        Loads as json object to be read
        Returns the timestamp object
    """  
    try:
        response = client.get_object(
            Bucket=bucket_name,
            Key=f'timestamp/{table}-last_extracted_timestamp.txt'
        )
        last_extracted_time = json.loads(response['Body'].read())
        return last_extracted_time
    except:
        create_new_extract(bucket_name, table, client)
        response = client.get_object(
            Bucket=bucket_name,
            Key=f'timestamp/{table}-last_extracted_timestamp.txt'
        )
        last_extracted_time = json.loads(response['Body'].read())
        return last_extracted_time