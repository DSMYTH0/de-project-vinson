import pytest
import os
import boto3
import json
import pandas as pd
from moto import mock_aws
from unittest.mock import patch, Mock
from pg8000.exceptions import DatabaseError
from src.extract.extract import (                        
                        extract_handler, 
                        put_csv_object,
                        extract_data,
                        fetch_table_names
                    )


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
    bucket_name = "test_bucket"
    s3_client.create_bucket(
        Bucket= bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return bucket_name


class TestUtilFunctions:
    
    @pytest.mark.it("unit test: fetch_table_names function returns lists of table names")
    def test_function_return_list_of_table_names(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as patch_connection:
            #arrange
            mock_connection = Mock()
            patch_connection.return_value = mock_connection
            mock_connection.run.return_value = [['Test_table-1'], ['Test_table-1']]
            
            response = fetch_table_names(mock_connection)
            
            assert 'Test_table-1' in response
            assert len(response) == 2
            assert isinstance(response, list)
    
    
    @pytest.mark.it("unit test: extract_data function checks expected columns in response and returns data type if extracted successfully")
    def test_function_return_expected_columns_and_check_data_type(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as patch_connection:
            #patch table_names
            with patch('src.extract.extract.fetch_table_names') as patch_table_names:
                #arrange
                mock_connection = Mock()
                patch_connection.return_value = mock_connection
                patch_table_names.return_value = ['test_table']
                mock_connection.run.return_value = [
                    [1, 'value-1', 'value-2', 'value-3', 'value-4']
                ]
                mock_connection.columns = [
                     {'name': 'column-id'},
                     {'name': 'column-1'},
                     {'name': 'column-2'},
                     {'name': 'column-3'},
                     {'name': 'column-4'},
                ]
                #act
                response = extract_data(mock_connection, 'test_table')

                #assert
                assert 'column-id' in response
                assert isinstance(response, pd.DataFrame)
    

                
class TestExtractHandler:
    
    @pytest.mark.it("unit test: function retuns the name of mocked bucket")
    def test_function_return_the_name_of_the_bucket(self, s3_client, s3_bucket):
        response = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        assert s3_bucket in bucket_names
        
    
    @pytest.mark.it("unit test: function checks bucket is empty")
    def test_function_checks_bucket_is_empty(self, s3_client, s3_bucket):
        bucket_name = s3_client.list_objects_v2(Bucket="test_bucket")
        assert bucket_name['KeyCount'] == 0
    
    
    @pytest.mark.it("unit test: put_csv_object function puts csv file into s3 bucket")
    def test_function_puts_csv_file_into_bucket(self, s3_client, s3_bucket):
            csv_file = """
                1,6826 Herzog Via,,Avon,New Patienceburgh,28441,Turkey,1803 637401,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962\n
                2,179 Alexie Cliffs,,,Aliso Viejo,99305-7380,San Marino,9621 880720,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962\n
            """
            file_path = 'test_file.csv'
      
            result = put_csv_object(csv_file, s3_bucket, file_path, client=s3_client)
            assert result is True
    
    
    
    @pytest.mark.it("unit test: function verify if there is any DatabaseError with statusCode 500")
    def test_function_raises_DatabaseError_with_statusCode_500(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as patch_connection:
            #patch logger
            with patch('src.extract.extract.logging.getLogger') as patch_logging:
                #arrange
                patch_connection.side_effect = DatabaseError
                mock_logger = Mock()
                patch_logging.return_value = mock_logger
                mock_logger.info.return_value = None
                
                #act
                response = extract_handler({}, {})
                
                #assert
                patch_connection.assert_called_once()
                mock_logger.error.assert_any_call('DatabaseError has been occurred')
                assert response['statusCode'] == 500
                assert response['body'] == 'DatabaseError has been occurred'
        
        
    @pytest.mark.it("unit test: function verify if there is other Exceptions with statusCode 404")
    def test_function_raises_Exception_with_statusCode_404(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as patch_connection:
            #patch logger
            with patch('src.extract.extract.logging.getLogger') as patch_logging:
                #arrange
                patch_connection.side_effect = Exception
                mock_logger = Mock()
                patch_logging.return_value = mock_logger
                mock_logger.info.return_value = None
                
                #act
                response = extract_handler({}, {})
                
                #assert
                patch_connection.assert_called_once()
                mock_logger.error.assert_any_call('Unexpected error has been occurred')
                assert response['statusCode'] == 404
                assert response['body'] == 'Unexpected error has been occurred'