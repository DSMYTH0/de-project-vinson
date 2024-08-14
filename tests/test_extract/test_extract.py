import pytest
import os
import boto3
import json
from moto import mock_aws
from unittest.mock import patch, Mock
from pg8000.exceptions import DatabaseError
from src.extract.extract import extract_handler, put_csv_object

# with patch.dict(os.environ, {"S3_BUCKET_NAME": "test_bucket"}):
#     from src.extract.extract import put_csv_object, fetch_table_names

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


class TestExtractLambda:
    
    @pytest.mark.it("unit test: function return response status if extracted successfully")
    def test_function_return_response_status(self, s3_bucket, s3_client):
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
                response = extract_handler({}, {})
                
                #assert
                patch_connection.assert_called_once()
                assert response['statusCode'] == 200
                assert response['body'] == 'Data has been extracted successfully'
    
             
                
    @pytest.mark.it("unit test: function verify logging call when invoked lambda")
    def test_function_varify_loggin(self):
        #patch logger
        with patch('src.extract.extract.logging.getLogger') as patch_logging:
            #arrange
            mock_logger = Mock()
            patch_logging.return_value = mock_logger
            mock_logger.info.return_value = None
            
            #act
            response = extract_handler({}, {})
            
            #assert
            assert response['statusCode'] == 200
            patch_logging.assert_called_once_with('extract_lambda_logger')
            mock_logger.info.assert_any_call('Data has been successfully extracted.')
            
            
    @pytest.mark.it("unit test: function varify if there is any DatabaseError")
    def test_function_raise_DatabaseError(self):
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
                mock_logger.error.assert_any_call('Error has been occurred')
                assert response['statusCode'] == 500
                assert response['body'] == 'error has been occurred'
                
                
                
class TestPutObject:
    @pytest.mark.it("unit test: function retuns the name of mocked bucket")
    def test_function_return_the_name_of_the_bucket(self, s3_client, s3_bucket):
        response = s3_client.list_buckets()
        bucket_names = [bucket['Name'] for bucket in response['Buckets']]
        assert s3_bucket in bucket_names
        
    
    @pytest.mark.it("unit test: function checks bucket is empty")
    def test_function_checks_bucket_is_empty(self, s3_client, s3_bucket):
        bucket_name = s3_client.list_objects_v2(Bucket="test_bucket")
        assert bucket_name['KeyCount'] == 0
        
    @pytest.mark.it("unit test: function puts csv file into bucket")
    def test_function_puts_csv_file_into_bucket(self, s3_client, s3_bucket):
            csv_file = """
                1,6826 Herzog Via,,Avon,New Patienceburgh,28441,Turkey,1803 637401,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962\n
                2,179 Alexie Cliffs,,,Aliso Viejo,99305-7380,San Marino,9621 880720,2022-11-03 14:20:49.962,2022-11-03 14:20:49.962\n
            """
            file_path = 'test_file.csv'
      
            result = put_csv_object(csv_file, s3_bucket, file_path, client=s3_client)
            assert result is True
            
        