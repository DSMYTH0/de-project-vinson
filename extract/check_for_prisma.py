import boto3
import json
from pprint import pprint

def check_prisma(bucket_name, table, client=boto3.client('s3')):
    response = client.list_objects_v2(
        Bucket=bucket_name
    )
    files = [response['Contents'][i]['Key'] for i in range(len(response['Contents']))]
    for i in files:
        if "_prisma_migrations-" in i and ".csv" in i:
            return
    return "SELECT * FROM _prisma_migrations;"