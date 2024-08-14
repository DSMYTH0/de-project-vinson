from src.utils.connection import connect_to_db
from pg8000.exceptions import DatabaseError
from datetime import datetime
import logging
import pandas as pd
import boto3


#Create function to fetch all table names from database
def fetch_table_names(conn):
    tables = (conn.run("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
    print(tables)
    return [table for item in tables for table in item]


#Create funciton to extract dat from database
def extract_data(conn, table):
    query = f"SELECT * FROM {table}"
    response = conn.run(query)
    columns = [column['name'] for column in conn.columns]
    df = pd.DataFrame(data=response, columns=columns)
    return df

# Create function to put csv file into s3 bucket
def put_csv_object(body, bucket, key_name, client=boto3.client('s3')):
     
        client.put_object(
            Body=body,
            Bucket=bucket,
            Key=key_name,
        )
        return True


def extract_handler(event, context):
    #Create logging object for cloudWatch
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)
    
    conn = None 
    
    current_date = datetime.now()
    
    try:
        #Create db connection
        conn = connect_to_db()
        
        bucket_name = 'vinson-landing-zone'
        tables = fetch_table_names(conn)
        #Check tables not empty
        if not tables:
            logger.info('There is no table found to extract data.')
            return {
                'statusCode': 200,
                'body': 'There is no table found to extract data.'
            }
            
        #Iterate all tables to extract data
        for table in tables:
            extracted_df = extract_data(conn, table)
            csv_file = extracted_df.to_csv(index=False, lineterminator='\n')
            file_path = f'{table}/Year-{current_date.year}/Month-{current_date.month}/Day-{current_date.day}/{table}-{current_date.time()}.csv'

            put_csv_object(csv_file, bucket_name, file_path)

            logger.info(f'Data has been successfully extracted.')

        return {
            'statusCode': 200,
            'body': 'Data has been extracted successfully'
        }

    except DatabaseError:
        logger.error('DatabaseError has been occurred')
        return {
            'statusCode': 500,
            'body': 'DatabaseError has been occurred'
        }
        
    except Exception as e:
        logger.error('Unexpected error has been occurred')
        return {
            'statusCode': 404,
            'body': 'Unexpected error has been occurred',
            'error': e
        }
        
    #Finally close db connection if it's opened
    finally: 
        if conn:
            conn.close()
    
    

# print(extract_handler({}, {}))
