from connection import connect_to_db
from fetch_table_list import fetch_table_names
from get_last_extract import get_last_extracted_time 
from extract_data import extract_data
from put_csv import put_csv
import logging
from datetime import datetime, timezone






def extract_handler(event, response):
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)
    
    conn = None
    current_date = datetime.now(datetime.UTC)
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