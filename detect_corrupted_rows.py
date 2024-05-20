import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import json
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

def log_corrupted_row(corrupted_rows, row_id, error_message):
    corrupted_rows.append({
        'id': row_id,
        'error_message': error_message
    })

def connect_to_db():
    try:
        connection = mysql.connector.connect(**config)
        print("Successfully connected to MySQL")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def fetch_all_ids(table_name):
    connection = connect_to_db()
    if not connection:
        return [], []

    valid_ids = []
    corrupted_rows = []
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
        min_id, max_id = cursor.fetchone()

        current_id = min_id
        while current_id <= max_id:
            try:
                print(f"Fetching ID: SELECT id FROM {table_name} WHERE id = {current_id}")
                cursor.execute(f"SELECT id FROM {table_name} WHERE id = %s", (current_id,))
                row_id = cursor.fetchone()
                if row_id:
                    valid_ids.append(row_id[0])
                current_id += 1
            except Error as e:
                print(f"Error fetching ID at row {current_id}: {e}")
                log_corrupted_row(corrupted_rows, current_id, str(e))
                current_id += 2  # Skip the corrupted row

    except Error as e:
        print(f"Error during ID fetching: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

    return valid_ids, corrupted_rows

def check_table_for_corrupted_rows(table_name):
    valid_ids, corrupted_rows = fetch_all_ids(table_name)
    if not valid_ids:
        return

    for row_id in valid_ids:
        connection = connect_to_db()
        if not connection:
            break

        try:
            cursor = connection.cursor()
            print(f"Running query: SELECT * FROM {table_name} WHERE id = {row_id}")
            cursor.execute(f"SELECT * FROM {table_name} WHERE id = %s", (row_id,))
            cursor.fetchone()
        except Error as e:
            print(f"Error reading row {row_id}: {e}")
            log_corrupted_row(corrupted_rows, row_id, str(e))
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection closed")

    # Save corrupted rows to a JSON file
    json_filename = f'corrupted_{table_name}.json'
    with open(json_filename, 'w') as json_file:
        json.dump(corrupted_rows, json_file, indent=4)
        print(f"Corrupted rows logged to {json_filename}")
        print(f"Number of corrupted rows detected: {len(corrupted_rows)}")

def list_tables():
    connection = connect_to_db()
    if not connection:
        return []

    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return []
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def main():
    while True:
        print("\nDatabase Corruption Checker")
        print("1. List Tables")
        print("2. Check Table for Corruption")
        print("3. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            tables = list_tables()
            if tables:
                print("\nTables in the database:")
                for table in tables:
                    print(f"- {table}")
            else:
                print("No tables found or error connecting to the database.")

        elif choice == '2':
            table_name = input("Enter the table name to check for corruption: ")
            check_table_for_corrupted_rows(table_name)

        elif choice == '3':
            print("Exiting the program.")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
