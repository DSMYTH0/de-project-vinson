import logging
import pandas as pd
import awswrangler as wr
import boto3


logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)


def transform_handler(event, context):

    try:
        s3_client = boto3.client('s3')
        ingestion_bucket = "vinson-ingestion-zone"
        processed_bucket = "vinson-processed-zone"

        ingestion_tables = ['design', 'currency', 'staff', 'counterparty', 'address', 'department', 'sales_order']

        df_dict = {}
        #for loop? util functions?

        star_schema_tables = dict()

        star_schema_tables['dim_date'] = dim_date()
        star_schema_tables['dim_counterparty'] = dim_counterparty()
        star_schema_tables['dim_staff'] = dim_staff()
        star_schema_tables['dim_location'] = dim_location()
        star_schema_tables['dim_design'] = dim_design()
        star_schema_tables['dim_currency'] = dim_currency()
        star_schema_tables['fact_sales_order'] = fact_sales_order()

    
    except Exception:
            response = logger.error("-ERROR- Data processing failed")
            return response