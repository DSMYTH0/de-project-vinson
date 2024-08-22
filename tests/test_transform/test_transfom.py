import os
import pytest
import boto3
import pandas as pd
from moto import mock_aws
from unittest.mock import patch
from src.transform.transform import dim_currency, currencies, return_dataframes, read_csv_from_s3


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
        s3 = boto3.client("s3", region_name="eu-west-2")
        yield s3


@pytest.fixture(scope="function")
def s3_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client

def mock_return_dataframes(bucket_name):
    df = pd.DataFrame({
        'currency_id': [1, 2],
        'currency_code': ['USD', 'EUR'],
        'created_at': ['2023-01-01', '2023-01-02'],
        'last_updated': ['2023-02-01', '2023-02-02'],
    })
    return [df]


def mock_currencies():
    return {
        'USD': 'US Dollar',
        'EUR': 'Euro'
    }

class TestDimCurrency:
    def test_currencies_util(self):
        assert currencies()["GBP"] == "Pound Sterling"
        assert currencies()["USD"] == "US Dollar"
        assert currencies()["EUR"] == "Euro"
        assert currencies()["VED"] == "Venezuelan digital bolivar"
        assert currencies()["SVC"] == "El Salvador Colon"

    def test_works_as_intended(self, s3_bucket):
        result = dim_currency(return_dataframes_func=mock_return_dataframes, currencies_func=mock_currencies)
        expected_df = pd.DataFrame({
            'currency_id': [1, 2],
            'currency_code': ['USD', 'EUR'],
            'currency_name': ['US Dollar', 'Euro']
        })

        pd.testing.assert_frame_equal(result, expected_df)



class TestReturnDataframes():
    def test_return_dataframes_gives_dataframe_of_correct_format(self, s3_bucket):
        csv_content = """id,name,value
                        1,Alice,100
                        2,Bob,200
                        3,Charlie,300"""

        s3_bucket.put_object(
            Body=csv_content,
            Bucket="test-bucket", 
            Key='table1.csv', 
            )
        
        expected_df = pd.DataFrame({
                        'id': [1, 2, 3],
                        'name': ['Alice', 'Bob', 'Charlie'],
                        'value': [100, 200, 300]
        })
        result = read_csv_from_s3('test-bucket', 'table1.csv')
        
        pd.testing.assert_frame_equal(result, expected_df)
