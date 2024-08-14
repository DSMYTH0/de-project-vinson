from src.extract.extract import extract_handler
import pytest
from unittest.mock import patch, Mock
from pg8000.exceptions import DatabaseError


class TestExtractLambda:
    
    @pytest.mark.it("unit test: function return response status if extracted successfully")
    def test_function_return_response_status(self):
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