from src.utils.connection import connect_to_db
from pg8000.exceptions import DatabaseError
from datetime import datetime
import logging
import pandas as pd
import boto3
# import io


#Create function to fetch all table names from database
def fetch_table_names(conn):
    tables = (conn.run("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
    return [table for item in tables for table in item]



def extract_handler(event, context):
    #Create logging object for cloudWatch
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)
    
    conn = None 
    
    try:
        #Create db connection
        conn = connect_to_db()
        
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
            query = f"SELECT * FROM {table}"
            response = conn.run(query)
            columns = [column['name'] for column in conn.columns]
            df = pd.DataFrame(data=response, columns=columns)
            csv_file = df.to_csv(index=False, lineterminator='\n')
            
            logger.info('Data has been successfully extracted.')

        return {
            'statusCode': 200,
            'body': 'Data has been extracted successfully'
        }

    except DatabaseError:
        logger.error('Error has been occurred')
        return {
            'statusCode': 500,
            'body': 'error has been occurred'
        }
    #Finally close db connection if it's opened
    finally: 
        if conn:
            conn.close()
    
    

print(extract_handler({}, {}))