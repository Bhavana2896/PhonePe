

def push_data_into_mysql(conn, cursor, dfs, table_columns):
    for table_name in dfs.keys():
        df = dfs[table_name]
        columns = table_columns[table_name]
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        for _, row in df.iterrows():
            data = tuple(row[column] for column in columns)
            cursor.execute(query, data)
        conn.commit()
    print("Data successfully pushed into MySQL tables")

# Mapping my_sql tables to pandas dataframes that we have created earlier



