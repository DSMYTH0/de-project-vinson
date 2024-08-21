import logging
#Create function to fetch all table names from database
def fetch_table_names(conn):
    """ 
        Function takes an db connection argument.
        Run connection with query string to fetch table names from database.
        Function will fetch all the table names if connection is done.
        It returns a list of table names.
    """
    tables = (conn.run("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """))
    if tables:
        return [table for item in tables for table in item]
    else:
        logging.error('No tables in the database')