from src.extract.extract import extract_handler
import pytest



class TestLambdaHandler:
    
    # @pytest.mark.it("unit test: function raises database connection error if not connected to database")
    # def test_function_raises_database_error(self):
    #     with pytest.raises(DatabaseError):
    #         assert lambda_handler() == {'message': 'connection error happened'}
    
    @pytest.mark.it("unit test: function returns table names when a query to database")
    def test_function_returns_expected_list_of_table_names(self):
        excepted_tables = ['address', 'staff', 'payment']
        response = extract_handler()
        for table in excepted_tables:
            assert table in response
            
    