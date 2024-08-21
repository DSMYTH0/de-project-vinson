from .sql_queries import get_last_updated_data
from .update_extract import update_extracted_time
from .get_last_extract import get_last_extracted_time
import pandas as pd
import logging
import boto3




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
