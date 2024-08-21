import pytest
from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import boto3
import awswrangler as wr
import os
import unittest
from src.transform.transform_utils.utils import return_dataframes, read_csv_from_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")
       
@pytest.fixture(scope="function")
def s3_bucket(s3_client):
    s3_client.create_bucket(
        Bucket= "test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3_client



class TestReadObjectFromBucket():
#review this test - is it passing for the right reasons?
    def test_read_object_from_bucket(self, s3_bucket):
        # csv_content = """id,name,value
        #                 1,Alice,100
        #                 2,Bob,200
        #                 3,Charlie,300"""

        # response = s3_bucket.put_object(
        #     Body='csv_content',
        #     Bucket="test_bucket", 
        #     Key='table1.csv', 
        #     )

        mock_df = pd.DataFrame({
                'column1': [1, 2, 3],
                'column2': ['a', 'b', 'c']
            })
        # Mocking the return values
        with patch('src.extract.extract.fetch_table_names') as mock_tables:
            with patch('src.transform.transform_utils.utils.read_csv_from_s3') as mock_read_csv:
                mock_tables.return_value = ['table1.csv']
                mock_read_csv.return_value = mock_df
                
                df_list = return_dataframes('test-t')
                #print(df_list)
                
                assert len(df_list) == 11
                assert isinstance(df_list, list)
                #mock_read_csv.assert_any_call('s3://test_bucket/table1.csv') 

   
   
    def test_returns_error_if_bucket_does_not_exist(self, s3_bucket):
        pass