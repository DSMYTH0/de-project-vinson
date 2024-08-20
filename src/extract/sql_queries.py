def get_last_updated_data(table):
    return f"""SELECT * FROM {table}
                WHERE last_updated > :time
                ORDER BY last_updated DESC;"""