import pytest
import os
import boto3
import json
import pandas as pd
from moto import mock_aws
from unittest.mock import patch, Mock, MagicMock, ANY
from pg8000.exceptions import DatabaseError
from src.extract.extract import extract_handler
from src.extract.fetch_table_list import fetch_table_names
from src.extract.get_last_extract import get_last_extracted_time
from src.extract.update_extract import update_extracted_time
from src.extract.extract_data import extract_data
from src.extract.put_csv import put_csv
from datetime import datetime, timezone



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
        Bucket= "vinson-ingestion-zone",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture(scope="function")
def sample_dataframe():
    data = {
        'column1': range(1, 11),
        'column2': [f'name{i}' for i in range(1, 11)], 
        'column3': [x * 1.5 for x in range(10)],
        'column4': [True, False] * 5,
        'column5': pd.date_range('20230101', periods=10), 
        'column6': list('ABCDEFGHIJ'),
        'column7': pd.Series(range(10)).astype('category'),
        'column8': [None] * 5 + list(range(5)), 
        'column9': [0.1 * i for i in range(10)],
        'column10': [f'group{i % 3}' for i in range(10)]
    }

    df = pd.DataFrame(data)
    return df

@pytest.fixture(scope="function")
def sample_small_dataframe():
    data = {
        'column1': [1], 
        'column2': ['name1'], 
        'column3': [0.1],  
        'column4': [True],        
        'column5': ['2023-01-01'], 
        'column6': ['A'],      
        'column7': ['category'],  
        'column8': [1],  
        'column9': [0.1],  
        'column10': ['label']  
    }

    df = pd.DataFrame(data)
    return df



class TestUtilFunctions:
    
    @pytest.mark.it("unit test: fetch_table_names function checks the database is empty")
    def test_function_checks_the_database_is_empty(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            with patch('logging.error') as mock_log_error:
                mock_connection.run.return_value = []
                
                fetch_table_names(mock_connection)
                
                mock_log_error.assert_called_once_with('No tables in the database')
            
    
    
    @pytest.mark.it("unit test: fetch_table_names function returns lists of table names")
    def test_function_return_list_of_table_names(self):
        #patch connection
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            mock_connection.run.return_value = [["column_one"], ["column_two"], ["column_three"], ["column_four"]]
            
            response = fetch_table_names(mock_connection)
            
            assert "column_one" in response
            assert len(response) == 4
            assert isinstance(response, list)
            for result in response:
                assert isinstance(result, str)
    
    
    @pytest.mark.it("unit test: get_last_extracted_time function returns expected time when a file is available in timestamp directory")
    def test_function_return_expected_time(self, s3_client, s3_bucket):
        s3_client.put_object(
        Body=json.dumps("Hello this is a test string"),
        Bucket="vinson-ingestion-zone",
        Key=f'timestamp/staff-last_extracted_timestamp.txt'
        )
        
        response = get_last_extracted_time("vinson-ingestion-zone", "staff", s3_client)

        assert response == "Hello this is a test string"
        
    @pytest.mark.it("unit test: get_last_extracted_time function returns expected very old time when no file is available in timestamp directory")
    def test_function_return_expected_time_when_no_timestamp_file(self, s3_client, s3_bucket):
        response = get_last_extracted_time("vinson-ingestion-zone", "staff", s3_client)

        assert response == "2000-01-01 01:01:01"
    
    
    @pytest.mark.it("unit test: update_extracted_time function replaces the timestamp file within the bucket when new data is detected")
    def test_function_updates_timestamp_txt(self, s3_client, s3_bucket):
        s3_client.put_object(
        Body=json.dumps("Hello this is a test string"),
        Bucket="vinson-ingestion-zone",
        Key=f'timestamp/staff-last_extracted_timestamp.txt'
        )
        update_extracted_time("vinson-ingestion-zone", "staff", "string to test")
        response = get_last_extracted_time("vinson-ingestion-zone", "staff", s3_client)
        
        assert response == "string to test"
        
    
    @pytest.mark.it("unit test: extract_data function returns dataframe if updates available")
    def test_function_returns_expected_dataframe(self):
        bucket = "vinson-ingestion-zone"
        current_date = "2024-08-19 15:04:40.584900+00:00"
            
        
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            with patch('src.extract.extract_data.get_last_extracted_time') as mock_extract_time:
                with patch('src.extract.extract_data.update_extracted_time') as mock_update_time:
                    mock_extract_time.return_value = "2024-08-18 15:04:40.584900+00:00"  # A previous date
                    mock_update_time.return_value = None 
                
                    mock_connection.run.return_value = [[5, "word", 12]]
                    mock_connection.columns = [{'name':'colOne'}, {'name':'columnTwo'}, {'name':'col_3'}]
                
                
                    response = extract_data(mock_connection, "staff", bucket, current_date)
                
                
                    assert isinstance(response, pd.DataFrame)
                    assert list(response.columns) == ['colOne', 'columnTwo', 'col_3']
                    assert len(response) == len(mock_connection.run.return_value)
                    mock_update_time.assert_called_once_with(bucket, "staff", current_date)
                    
                    
    @pytest.mark.it("unit test: extract_data function returns expected output if no updates available")
    def test_function_returns_expected_message(self, s3_bucket):
        bucket = "vinson-ingestion-zone"
        current_date = "2024-08-19 15:04:40.584900+00:00"
            
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            mock_connection.run.return_value = []
            with patch('logging.info') as mock_log_info:
                extract_data(mock_connection, "staff", bucket, current_date)
        
        
                mock_log_info.assert_called_once_with('No new staff data found')
                
                
    @pytest.mark.it("unit test: put_csv function puts csv file into bucket")
    def test_function_puts_csv_in_bucket(self, s3_client, s3_bucket, sample_dataframe):
        bucket = "vinson-ingestion-zone"
        csv_file = sample_dataframe.to_csv(index=False, lineterminator='\n')
        file_path = "test/path.csv"
        
        put_csv(csv_file, bucket, file_path)
        

        response = s3_client.get_object(Bucket=bucket, Key=file_path)
        body = response['Body'].read().decode('utf-8')

        assert body == csv_file
        
        objects = s3_client.list_objects_v2(Bucket=bucket)
        
        assert objects['Contents'][0]['Key'] == file_path
        
        

    
    @pytest.mark.it("integration test: extract_handler function works as intended on first iteration")
    def test_function_returns_expected_on_first_execution(self, s3_bucket, sample_dataframe):
        event = {}
        response = {}
        bucket = "vinson-ingestion-zone" 
        
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            with patch('src.extract.extract.fetch_table_names') as mock_fetch:
                with patch('src.extract.extract.extract_data') as mock_extract_data:
                    with patch('src.extract.extract.put_csv') as mock_put_csv:
                        
                        with patch('src.extract.extract.datetime') as mock_datetime:

                            # Set the mock datetime to a fixed point in time
                            mock_datetime.now.return_value = datetime(2024, 8, 19, 15, 4, 40, tzinfo=timezone.utc)
                            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                            
                            mock_connection.return_value = MagicMock()
                            mock_fetch.return_value = ['table1', 'table2', '_prisma_migrations']
                            mock_extract_data.return_value = sample_dataframe

                            extract_handler(event, response)
                            
                            expected_datetime = mock_datetime.now.return_value

                            mock_fetch.assert_called_once_with(mock_connection.return_value)
                            mock_extract_data.assert_any_call(mock_connection.return_value, 'table1', bucket, expected_datetime)
                            mock_extract_data.assert_any_call(mock_connection.return_value, 'table2', bucket, expected_datetime)
                            mock_put_csv.assert_any_call(ANY, ANY, ANY)
                            
                            for args in mock_extract_data.call_args_list:
                                assert args[0][1] != '_prisma_migrations'
                                
    @pytest.mark.it("integration test: extract_handler function works as intended on second iteration")
    def test_function_returns_expected_on_second_execution(self, s3_bucket, sample_dataframe, sample_small_dataframe):
        event = {}
        response = {}
        bucket = "vinson-ingestion-zone" 
        
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            with patch('src.extract.extract.fetch_table_names') as mock_fetch:
                with patch('src.extract.extract.extract_data') as mock_extract_data:
                    with patch('src.extract.extract.put_csv') as mock_put_csv:
                        with patch('src.extract.get_last_extract.create_new_extract') as mock_create_new:
                            with patch('src.extract.extract.datetime') as mock_datetime:

                                mock_datetime.now.return_value = datetime(2024, 8, 19, 15, 4, 40, tzinfo=timezone.utc)
                                mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                                
                                mock_connection.return_value = MagicMock()
                                mock_fetch.return_value = ['table1', 'table2', '_prisma_migrations']
                                mock_extract_data.return_value = sample_dataframe


                                extract_handler(event, response)
                                mock_extract_data.return_value = sample_small_dataframe
                                extract_handler(event, response)
                                
                                expected_datetime = mock_datetime.now.return_value

                                assert mock_put_csv.call_count == 4
                                
                                mock_extract_data.assert_any_call(mock_connection.return_value, 'table1', bucket, expected_datetime)
                                mock_extract_data.assert_any_call(mock_connection.return_value, 'table2', bucket, expected_datetime)
                                mock_put_csv.assert_any_call(ANY, ANY, ANY)
                                
                                for args in mock_extract_data.call_args_list:
                                    assert args[0][1] != '_prisma_migrations'
                                    
                                    
    @pytest.mark.it("integration test: extract_handler function works as intended when raising exception")
    def test_function_raises_exception(self, s3_bucket):
        event = {}
        response = {}
        bucket = "vinson-ingestion-zone" 
        
        with patch('src.extract.extract.connect_to_db') as mock_connection:
            with patch('src.extract.extract.fetch_table_names') as mock_fetch:
                with patch('src.extract.extract.extract_data') as mock_extract_data:
                    with patch('src.extract.extract.datetime') as mock_datetime:

                        mock_datetime.now.return_value = datetime(2024, 8, 19, 15, 4, 40, tzinfo=timezone.utc)
                        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
                        
                        mock_connection.return_value = MagicMock()
                        mock_fetch.return_value = ['table1', 'table2', '_prisma_migrations']
                        mock_extract_data.return_value = []


                        with patch('logging.error') as mock_log_error:
                            extract_handler(event, response)
                            mock_log_error.assert_called_once_with('Unexpected error raised during extract lambda function')