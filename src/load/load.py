import logging

def transform_handler(event, context):
    logger = logging.getLogger('Processing Lambda Log')
    logging.basicConfig()
    logger.setLevel(logging.INFO)
    logger.info("Lambda has been invoked")