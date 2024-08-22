import csv
import boto3
import logging

def transform_handler(event, context):
    logger = logging.getLogger('extract_lambda_logger')
    logger.setLevel(logging.INFO)

    logger.info("Transform Lambda had been invoked")