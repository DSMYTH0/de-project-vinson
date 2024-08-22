import pytest
from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import boto3
import awswrangler as wr
import os
import unittest
from src.transform.transform_utils.utils import return_dataframes, read_csv_from_s3, dim_staff, staff_df, dim_counterparty, counterparty_table


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

   
@pytest.fixture(scope="function")
def mock_staff_func():
    mock_staff_df = pd.DataFrame({
        "staff_id": [1, 2, 3],
        "first_name": ['first-1', 'first-2', 'first-3'],
        "last_name": ['last-1', 'last-2', 'last-3'],
        "department_id": [2, 5, 7],
        "email_address": ['admin@info.com', 'staff@info.com', 'team@info.com'],
        "created_at": ['2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355'],
        "last_updated": ['2024-08-21 10:13:01.761355', '2024-08-21 12:13:01.761355', '2024-08-21 14:13:01.761355']
    })
    return mock_staff_df

@pytest.fixture(scope="function")
def mock_department_func():
    mock_department_df = pd.DataFrame({
        "department_id": [2, 5, 7],
        "department_name": ['dev', 'admin', 'finance'],
        "location": ['Leeds', 'Kent', 'York'],
        "manager": ['Richard Roma', 'Mark Hanna', 'James Link'],
        "created_at": ['2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355'],
        "last_updated": ['2024-08-21 10:13:01.761355', '2024-08-21 12:13:01.761355', '2024-08-21 14:13:01.761355']
    })
    return mock_department_df

@pytest.mark.it('unit test: staff_df function returns expected dataFrame')
def test_function_returns_expected_staff_dataFrame(mock_staff_func):
    with patch('src.transform.transform_utils.utils.return_dataframes', return_value=[mock_staff_func]):
        expected_df = pd.DataFrame({
            "staff_id": [1, 2, 3],
            "first_name": ['first-1', 'first-2', 'first-3'],
            "last_name": ['last-1', 'last-2', 'last-3'],
            "email_address": ['admin@info.com', 'staff@info.com', 'team@info.com'],
            "department_id": [2, 5, 7]
        })
        expected_column_names = ['staff_id', 'first_name', 'last_name', 'email_address', 'department_id']
        response = staff_df()
        pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)
        for column in expected_column_names:
            assert column in response

@pytest.mark.it('unit test: dim_staff function returns expected dataFrame')
def test_function_returns_expected_department_dataFrame(mock_department_func):
    staff_df = pd.DataFrame({
         "staff_id": [1, 2, 3],
        "first_name": ['first-1', 'first-2', 'first-3'],
        "last_name": ['last-1', 'last-2', 'last-3'],
        "email_address": ['admin@info.com', 'staff@info.com', 'team@info.com'],
        "department_id": [2, 5, 7]
    })
    with patch('src.transform.transform_utils.utils.return_dataframes', return_value=[mock_department_func]):
        with patch('src.transform.transform_utils.utils.staff_df', return_value=staff_df):
            expected_df = pd.DataFrame({
                "department_name": ['dev', 'admin', 'finance'],
                "location": ['Leeds', 'Kent', 'York'],
                "staff_id": [1, 2, 3],
                "first_name": ['first-1', 'first-2', 'first-3'],
                "last_name": ['last-1', 'last-2', 'last-3'],
                "email_address": ['admin@info.com', 'staff@info.com', 'team@info.com']
            })
            response = dim_staff()
            print(response)
            pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)

@pytest.fixture(scope="function")
def mock_counterparty_table():
    mock_counterparty_df = pd.DataFrame({
        "counterparty_id": [1, 3, 4],
        "counterparty_legal_name": ['Counterparty1', 'Counterparty2', 'Counterparty3'],
        'legal_address_id': [5, 4, 3],
        'commercial_contact': ['Jack', 'Mike', 'John'],
        'delivery_contact': ['Jack', 'Mike', 'John'],
        "created_at": ['2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355', '2024-08-21 08:13:01.761355'],
        "last_updated": ['2024-08-21 10:13:01.761355', '2024-08-21 12:13:01.761355', '2024-08-21 14:13:01.761355']
    })
    return mock_counterparty_df

@pytest.fixture(scope="function")
def mock_address_func():
    mock_address_df = pd.DataFrame({
        'address_id': [11],
        "address_line_1": ["11 College Rd"],
        "address_line_2": ["Master House"],
        "district": ["Kent"],
        "city": ["Maidstone"],
        "postal_code": ["1289"],
        "country": ["England"],
        "phone": ["34512385743"],
        "created_at": ['2024-08-21 08:13:01.761355'],
        "last_updated": ['2024-08-21 10:13:01.761355']
    })
    return mock_address_df

@pytest.mark.it('unit test: counterparty_table function returns expected counterparty dataFrame')
def test_function_returns_expected_counterparty_dataFrame(mock_counterparty_table):
    with patch('src.transform.transform_utils.utils.return_dataframes', return_value=[mock_counterparty_table]):
        expected_df = pd.DataFrame({
            'legal_address_id': [5, 4, 3],
            "counterparty_id": [1, 3, 4],
            "counterparty_legal_name": ['Counterparty1', 'Counterparty2', 'Counterparty3']
        })
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'legal_address_id']
        response = counterparty_table()
        pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)
        assert [column in response.columns for column in expected_columns]

@pytest.mark.it('unit test: dim_counterparty function merges with address and returns expected counterparty dataFrame')
def test_function_merges_with_address_and_returns_expected_counterparty_dataFrame(mock_address_func):
    mock_counterparty_table = pd.DataFrame({
            'legal_address_id': [11],
            "counterparty_id": [1],
            "counterparty_legal_name": ['Counterparty1']
        })
    with patch('src.transform.transform_utils.utils.return_dataframes', return_value=[mock_address_func]):
        with patch('src.transform.transform_utils.utils.counterparty_table', return_value=mock_counterparty_table):
            expected_df = pd.DataFrame({
                'counterparty-legal_address_line_1': ["11 College Rd"],
                'counterparty-legal_address_line_2': ["Master House"],
                'counterparty_legal_district': ["Kent"],
                'counterparty_legal_city': ["Maidstone"],
                'counterparty_legal_postal_code': ["1289"],
                'counterparty_legal_country': ["England"],
                'counterparty_legal_phone_number': ["34512385743"],
                "counterparty_id": [1],
                'counterparty_legal_name': ["Counterparty1"]
            })
            expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_postal_code']
            response = dim_counterparty()
            pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)
            assert [column in response.columns for column in expected_columns]