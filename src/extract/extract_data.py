from src.extract.sql_queries import get_last_updated_data
from src.extract.update_extract import update_extracted_time
from src.extract.get_last_extract import get_last_extracted_time
import pandas as pd
import logging




def extract_data(conn, table, bucket_name, current_date):
    try:
        sql_string = get_last_updated_data(table)

        last_extract = get_last_extracted_time(bucket_name, table)
        
        response = conn.run(sql_string, time=last_extract)
        columns = [column['name'] for column in conn.columns]


        if response and columns:
            dataframe = pd.DataFrame(data=response, columns=columns)
            update_extracted_time(bucket_name, table, current_date)
            return dataframe
        else:
            logging.info(f'No new {table} data found')
    except Exception as e:
        logging.error(f'Unexpected error raised during extract data function: {e}')
