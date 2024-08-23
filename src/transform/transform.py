import logging
import pandas as pd
import awswrangler as wr
import boto3
from src.transform.transform_utils.utils import data_to_parquet, dim_design, dim_location, dim_staff, dim_counterparty, dim_currency, dim_date, fact_sales_order


logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)


def transform_handler(event, context):

    try:
        s3_client = boto3.client('s3')
        ingestion_bucket = "vinson-ingestion-zone"
        processed_bucket = "vinson-processed-zone"

        star_schema_tables = {}

        star_schema_tables['dim_date'] = dim_date()
        star_schema_tables['dim_counterparty'] = dim_counterparty()
        star_schema_tables['dim_staff'] = dim_staff()
        star_schema_tables['dim_location'] = dim_location()
        star_schema_tables['dim_design'] = dim_design()
        star_schema_tables['dim_currency'] = dim_currency()
        star_schema_tables['fact_sales_order'] = fact_sales_order()

        for table, df in star_schema_tables.items():
            data_to_parquet(table, df, processed_bucket)
    
    except Exception as e:
            logger.error("-ERROR- Data processing failed")
        
transform_handler({}, {})