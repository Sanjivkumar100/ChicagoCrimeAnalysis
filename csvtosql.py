import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_table_and_insert(csv_file_path, table_name, host, user, password, database):
    try:
        # Load the CSV into a Pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Check for separate date and time columns
        has_date_column = 'date' in df.columns
        has_time_column = 'time' in df.columns

        # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = connection.cursor()

        # Generate SQL table creation statement based on DataFrame
        columns = []
        for col in df.columns:
            # Infer data type
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                sql_type = "INT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "FLOAT"
            elif has_date_column and col == 'date':
                sql_type = "DATE"
            elif has_time_column and col == 'time':
                sql_type = "TIME"
            else:
                max_length = max(df[col].astype(str).apply(len).max(), 255)
                sql_type = f"VARCHAR({max_length})"
            columns.append(f"`{col}` {sql_type}")

        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)});"
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created successfully!")

        # Insert data into the table in batches
        batch_size = 1000  # Number of rows per batch
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
        
        for start in range(0, len(df), batch_size):
            end = start + batch_size
            cursor.executemany(insert_query, df.iloc[start:end].values.tolist())
            connection.commit()
            print(f"Inserted rows {start} to {min(end, len(df)) - 1}")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")

# Example Usage
create_table_and_insert(
    csv_file_path="ccadatacleaned.csv",  # Path to your uploaded CSV file
    table_name="cca",            # Table name to create
    host="localhost",                             # MySQL host
    user="root",                                  # MySQL username
    password="1234",                     # MySQL password
    database="chicagocrimeanalysis"                      # MySQL database name
)
