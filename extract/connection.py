import boto3
from pg8000.native import Connection


def connect_to_db(sm=boto3.client('secretsmanager', region_name="eu-west-2")):
    print()
    return Connection(
        user=sm.get_secret_value(SecretId='pg_user')['SecretString'],
        password=sm.get_secret_value(SecretId='pg_password')['SecretString'],
        database=sm.get_secret_value(SecretId='pg_database')['SecretString'],
        host=sm.get_secret_value(SecretId='pg_host')['SecretString'],
        port=sm.get_secret_value(SecretId='pg_port')['SecretString']
    )

