from src.utils.connection import connect_to_db
from pg8000.exceptions import DatabaseError
from datetime import datetime
import logging
import pandas as pd
import boto3


#Create function to fetch all table names from database
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
        return ('No tables in the database')


#Create funciton to extract dat from database
def extract_data(conn, table):
    """ 
        Function takes two arguments (e.g db connection and table name).
        Initialise a query string with the table name.
        Run connection with query string to extract data from database.
        Get all the column names for the table.
        Covert response data and columns into a dataFrame.
        Finally return the dataFrame
    """
    query = f"SELECT * FROM {table}"
    response = conn.run(query)
    columns = [column['name'] for column in conn.columns]
    if response and columns:
        df = pd.DataFrame(data=response, columns=columns)
        return df

        

# Create function to put csv file into s3 bucket
def put_csv_object(body, bucket, key_name, client=boto3.client('s3')):
    """ 
        Function takes four arguments:
            - body : data like csv_file
            - bucket: to put object into a bucket
            - key_name: file path for the object
            - client: a default boto3 client to put object into bucket 
    """
     
    client.put_object(
        Body=body,
        Bucket=bucket,
        Key=key_name,
    )
    return True


def extract_handler(event, context):
    """ 
        Create lambda handler to extract data from source.
        Create loggig object to log info on cloudWath.
        Initially open a db connection object to connect to the database
        Get all the table names and check table name object is not empty.
        If there are tables than iterate through these tables.
        Call a function that extracts data from source database and return a dataFrame.
        Convert the dataFrame into csv file and put these csv file into S3 bucket.
        If successfull than log info on cloudWatch.
        Else raises DatabaseException or other Exception and handle those Exception.
        Also log these error on cloudWatch.
        Finally close the db connection if it is open.
    """
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
            if extract_data:
                csv_file = extracted_df.to_csv(index=False, lineterminator='\n')
                file_path = f'{table}/Year-{current_date.year}/Month-{current_date.month}/Day-{current_date.day}/{table}-{current_date.time()}.csv'
                print(csv_file)
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
#
