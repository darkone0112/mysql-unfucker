import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# Database connection configuration from environment variables
config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT', 3306)
}

def connect_to_db():
    while True:
        try:
            connection = mysql.connector.connect(**config)
            if connection.is_connected():
                print("Connected to MySQL")
                return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def transfer_rows(table_name, backup_table_name):
    connection = connect_to_db()
    cursor = connection.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {backup_table_name} LIKE {table_name};")
    cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
    min_id, max_id = cursor.fetchone()

    print("Processing from min to max...")
    last_successful_id = process_range(cursor, connection, table_name, backup_table_name, min_id, max_id, 1)

    cursor.close()
    connection.close()
    print("MySQL connection closed")
    print(f"Last successful insert from min to max: {last_successful_id}")
    return last_successful_id  # Return the last successful ID


def process_range(cursor, connection, table_name, backup_table_name, start, end, step):
    last_successful_id = None
    batch_size = 100
    current_id = start

    while current_id <= end:
        try:
            ids_to_process = list(range(current_id, min(current_id + batch_size, end + 1)))
            cursor.execute(f"""
                INSERT INTO {backup_table_name}
                SELECT * FROM {table_name}
                WHERE id IN ({','.join(map(str, ids_to_process))});
            """)
            connection.commit()
            last_successful_id = ids_to_process[-1]
            print(f"Successfully transferred rows up to id {last_successful_id}.")
            current_id += batch_size
        except Error as e:
            print(f"Error during row transfer up to id {last_successful_id}: {e}")
            if '2013' in str(e) and batch_size == 100:
                # Reduce to single-row processing if error occurs with batch size of 100
                batch_size = 1
            elif '2013' in str(e) and batch_size == 1:
                print(f"Encountered a corrupted row at ID {current_id}. Cannot proceed past this row.")
                break
            else:
                print("Unexpected error occurred. Reconnecting and retrying...")
                connection = connect_to_db()
                cursor = connection.cursor()

    return last_successful_id

if __name__ == "__main__":
    table_name = input("Enter the name of the corrupted table: ")
    backup_table_name = f"backup_{table_name}"
    transfer_rows(table_name, backup_table_name)
