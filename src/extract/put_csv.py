import boto3

def put_csv(body, bucket, file_path, client=boto3.client('s3')):
        
        client.put_object(
            Body=body,
            Bucket=bucket,
            Key=file_path,
        )