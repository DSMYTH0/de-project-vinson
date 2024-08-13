from src.utils.connection import connect_to_db
from pg8000.exceptions import DatabaseError
from datetime import datetime
from pprint import pprint


def fetch_table_names(conn):
    tables = (conn.run("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
    return [table for item in tables for table in item]



def extract_handler():
    try:
        conn = connect_to_db()
        # current_date = datetime.now()
        # file_name = f'TABLENAME/{current_date.year}/{current_date.month}/{current_date.day}/TABLENAME-{current_date.time()}.csv'
        tables = fetch_table_names(conn)
        table_data = []
        column_names = []
        for table in tables:
            query = f"SELECT * FROM {table}"
            response = conn.run(query)
            columns = [column['name'] for column in conn.columns]
            table_data.append(response)
            column_names.append(columns)
        return
    
    except DatabaseError:
        return {'message': 'connection error happened'}
    
    finally: 
        if conn:
            conn.close()
    
    

pprint(extract_handler())