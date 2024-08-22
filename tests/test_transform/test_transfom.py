import pytest
from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock
import pandas as pd
import boto3
import awswrangler as wr
import os
import unittest
from src.transform.transform_utils.utils import return_dataframes, read_csv_from_s3, dim_staff, staff_df, dim_counterparty, counterparty_table, currencies, dim_currency


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
        Bucket= "test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client




# class TestReadObjectFromBucket():
# # 

   
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

@pytest.mark.skip('unit test: dim_staff function returns expected dataFrame')
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

@pytest.mark.skip('unit test: dim_counterparty function merges with address and returns expected counterparty dataFrame')
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
            

@pytest.fixture(scope="function")
def mock_design_func():
    mock_design_df = pd.DataFrame({
        "design_id": [10],
        "created_at": ['2024-08-21 08:13:01.761355'],
        "last_updated": ['2024-08-21 10:13:01.761355'],
        "design_name": ["some design"],
        "file_location": ["some_location"],
        "file_name": ["file_name"]
    })
    return mock_design_df

@pytest.mark.skip('unit test: dim_design function returns expected design dataFrame')
def test_function_returns_expected_design_dataFrame(mock_design_func):
    with patch('src.transform.dim_tables.return_dataframes', return_value=[mock_design_func]):
        expected_df = pd.DataFrame({
            "design_id": [10],
            "design_name": ["some design"],
            "file_location": ["some_location"],
            "file_name": ["file_name"]
        })
        expected_columns = ['design_id', 'design_name', 'file_location', 'file_name']
        response = dim_design()
        pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)
        assert [column in response.columns for column in expected_columns]
@pytest.mark.skip('unit test: dim_location function returns expected location dataFrame')
def test_function_returns_expected_location_dataFrame(mock_address_func):
    with patch('src.transform.dim_tables.return_dataframes', return_value=[mock_address_func]):
        expected_df = pd.DataFrame({
            "location_id": [11],
            "address_line_1": ["11 College Rd"],
            "address_line_2": ["Master House"],
            "district": ["Kent"],
            "city": ["Maidstone"],
            "postal_code": ["1289"],
            "country": ["England"],
            "phone": ["34512385743"]
        })
        response = dim_location()
        pd.testing.assert_frame_equal(response, expected_df, check_dtype=False)
        assert 'location_id' in response.columns



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

    def test_works_as_intended(self):
        result = dim_currency(return_dataframes_func=mock_return_dataframes, currencies_func=mock_currencies)
        expected_df = pd.DataFrame({
            'currency_id': [1, 2],
            'currency_code': ['USD', 'EUR'],
            'currency_name': ['US Dollar', 'Euro']
        })
        expected_df.set_index('currency_id', inplace=True)
        print(result, "<<<<")
        print(expected_df, "<<<<<<<<")

        pd.testing.assert_frame_equal(result, expected_df)



class TestReturnDataframes():
    def test_return_dataframes_gives_dataframe_of_correct_format(self, s3_bucket):
        csv_content = """id,name,value
                        1,Alice,100
                        2,Bob,200
                        3,Charlie,300"""

        s3_bucket.put_object(
            Body=csv_content,
            Bucket="test_bucket", 
            Key='table1.csv', 
            )
        
        expected_df = pd.DataFrame({
                        'id': [1, 2, 3],
                        'name': ['Alice', 'Bob', 'Charlie'],
                        'value': [100, 200, 300]
        })
        result = read_csv_from_s3('test_bucket', 'table1.csv')
        
        pd.testing.assert_frame_equal(result, expected_df)