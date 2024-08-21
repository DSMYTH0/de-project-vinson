import json
import boto3
import pandas as pd
from pg8000.native import Connection

import logging
from datetime import datetime, timezone



def extract_handler(event, response):
    """The main entry point for the extract Lambda function.

    This function is responsible for the overall data extraction process. It connects to the database, fetches the list of tables,
    extracts data from each table (only when new data is available), and uploads the data as CSV files to an S3 bucket.

    Args:
        event (dict): The event object that triggered the Lambda function.
        response (dict): The response object to be returned by the Lambda function.

    Raises:
        Exception: If an unexpected error occurs during the extraction process.
    """
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)
    
    conn = None
    current_date = datetime.now(timezone.utc)
    print(current_date)
    bucket = "vinson-ingestion-zone"

    
    try:
        conn = connect_to_db()
        table_list = fetch_table_names(conn)
        if '_prisma_migrations' in table_list:
            table_list.remove('_prisma_migrations')
                 
        for table in table_list:
            data_frame = extract_data(conn, table, bucket, current_date)
        
            file_path = f'{table}/Year-{current_date.year}/Month-{current_date.month}/Day-{current_date.day}/{table}-{current_date.time()}.csv'
            if data_frame is not None:
                csv_file = data_frame.to_csv(index=False, lineterminator='\n')
                put_csv(csv_file, bucket, file_path)
                logger.info(f'{table} - Updated')

    except Exception as e:
        logging.error(f'Unexpected error raised during extract lambda function')


# Connection.py

def connect_to_db(sm=boto3.client('secretsmanager', region_name="eu-west-2")):
    """Establishes a connection to the database using the secrets stored in AWS Secrets Manager.

    Args:
        sm (boto3.client): An optional AWS Secrets Manager client. If not provided, a new client will be created.

    Returns:
        pg8000.native.Connection: A database connection object.
    """
    return Connection(
        user=sm.get_secret_value(SecretId='pg_user')['SecretString'],
        password=sm.get_secret_value(SecretId='pg_password')['SecretString'],
        database=sm.get_secret_value(SecretId='pg_database')['SecretString'],
        host=sm.get_secret_value(SecretId='pg_host')['SecretString'],
        port=sm.get_secret_value(SecretId='pg_port')['SecretString']
    )


# Fetch table list.py
def fetch_table_names(conn):
    """Fetches the list of table names from the database.

    Args:
        conn (pg8000.native.Connection): A database connection object.

    Returns:
        list: A list of table names.

    Raises:
        logging.error: If there are no tables in the database.
    """
    tables = (conn.run("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
    if tables:
        return [table for item in tables for table in item]
    else:
        logging.error('No tables in the database')


# Extract data.py
def extract_data(conn, table, bucket_name, current_date, client=boto3.client('s3')):
    """Extracts data from a specified table in the database and uploads it as a CSV file to an S3 bucket.

    Args:
        conn (pg8000.native.Connection): A database connection object.
        table (str): The name of the table to extract data from.
        bucket_name (str): The name of the S3 bucket to upload the data to.
        current_date (datetime.datetime): The current date and time.
        client (boto3.client): An optional AWS S3 client. If not provided, a new client will be created.

    Returns:
        pandas.DataFrame: The extracted data as a DataFrame, or None if no new data was found.

    Raises:
        logging.error: If an unexpected error occurs during the data extraction process.
    """
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)

    try:
        sql_string = get_last_updated_data(table)

        last_extract = get_last_extracted_time(bucket_name, table, client)
        
        response = conn.run(sql_string, time=last_extract)
        columns = [column['name'] for column in conn.columns]


        if response and columns:
            dataframe = pd.DataFrame(data=response, columns=columns)
            update_extracted_time(bucket_name, table, current_date, client)
            return dataframe
        else:
            logger.info(f'No new {table} data found')
    except Exception:
        logging.error(f'Unexpected error raised during extract data function')


# sql_queries.py
def get_last_updated_data(table):
    """Returns an sql query
    
    Args:
        table (str): The name of the table from which updated files are located
    """
    return f"""SELECT * FROM {table}
                WHERE last_updated > :time
                ORDER BY last_updated DESC;"""


# get_last_extract.py
def get_last_extracted_time(bucket_name, table, client=boto3.client('s3')):
    """Retrieves the last extracted timestamp for a given table from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        table (str): The name of the table.
        client (boto3.client): An optional AWS S3 client. If not provided, a new client will be created.

    Returns:
        str: The last extracted timestamp as a string, or a default value of '2000-01-01 01:01:01' if no timestamp is found.
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
    
# create_new_extract.py
def create_new_extract(bucket_name, table, client=boto3.client('s3')):
    """Creates a new timestamp file in an S3 bucket for a given table.

    Args:
        bucket_name (str): The name of the S3 bucket.
        table (str): The name of the table.
        client (boto3.client): An optional AWS S3 client. If not provided, a new client will be created.
    """
    client.put_object(
    Body=json.dumps(str(datetime(2000, 1, 1, 1, 1, 1, 0))),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )


# update_extract.py
def update_extracted_time(bucket_name, table, current_date, client=boto3.client('s3')):
    """Updates the last extracted timestamp for a given table in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        table (str): The name of the table.
        current_date (datetime.datetime): The current date and time.
        client (boto3.client): An optional AWS S3 client. If not provided, a new client will be created.
    """
    client.put_object(
    Body=json.dumps(str(current_date)),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )


# put_csv.py
def put_csv(body, bucket, file_path, client=boto3.client('s3')):
    """Uploads a CSV file to an S3 bucket.

    Args:
        body (str): The contents of the CSV file.
        bucket (str): The name of the S3 bucket.
        file_path (str): The path to the file within the bucket.
        client (boto3.client): An optional AWS S3 client. If not provided, a new client will be created.
    """ 
    client.put_object(
        Body=body,
        Bucket=bucket,
        Key=file_path,
    )