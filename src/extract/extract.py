import json
import boto3
import pandas as pd
from pg8000.native import Connection

import logging
from datetime import datetime, timezone



def extract_handler(event, response):
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
    print()
    return Connection(
        user=sm.get_secret_value(SecretId='pg_user')['SecretString'],
        password=sm.get_secret_value(SecretId='pg_password')['SecretString'],
        database=sm.get_secret_value(SecretId='pg_database')['SecretString'],
        host=sm.get_secret_value(SecretId='pg_host')['SecretString'],
        port=sm.get_secret_value(SecretId='pg_port')['SecretString']
    )


# Fetch table list.py
def fetch_table_names(conn):
    """ 
        Function takes an db connection argument.
        Run connection with query string to fetch table names from database.
        Function will fetch all the table names if connection is done.
        It returns a list of table names.
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
            logging.info(f'No new {table} data found')
    except Exception as e:
        logging.error(f'Unexpected error raised during extract data function: {e}')


# sql_queries.py
def get_last_updated_data(table):
    return f"""SELECT * FROM {table}
                WHERE last_updated > :time
                ORDER BY last_updated DESC;"""


# get_last_extract.py
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
    
# create_new_extract.py
def create_new_extract(bucket_name, table, client=boto3.client('s3')):
    client.put_object(
    Body=json.dumps(str(datetime(2000, 1, 1, 1, 1, 1, 0))),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )


# update_extract.py
def update_extracted_time(bucket_name, table, current_date, client=boto3.client('s3')):
    client.put_object(
    Body=json.dumps(str(current_date)),
    Bucket=bucket_name,
    Key=f'timestamp/{table}-last_extracted_timestamp.txt'
    )


# put_csv.py
def put_csv(body, bucket, file_path, client=boto3.client('s3')):
        
        client.put_object(
            Body=body,
            Bucket=bucket,
            Key=file_path,
        )