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

def fetch_id_range(table_name):
    connection = connect_to_db()
    if not connection:
        return None, None

    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT MIN(id), MAX(id) FROM {table_name}")
        min_id, max_id = cursor.fetchone()
        return min_id, max_id
    except Error as e:
        print(f"Error fetching ID range: {e}")
        return None, None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def check_id_range(table_name, start_id, end_id):
    connection = connect_to_db()
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name} WHERE id BETWEEN %s AND %s"
        print(f"Running query: {query} with {start_id} and {end_id}")
        cursor.execute(query, (start_id, end_id))
        cursor.fetchall()
        return True
    except Error as e:
        print(f"Error checking ID range {start_id} to {end_id}: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def check_individual_id(table_name, row_id):
    connection = connect_to_db()
    if not connection:
        return False

    try:
        cursor = connection.cursor()
        query = f"SELECT * FROM {table_name} WHERE id = %s"
        print(f"Running query: {query} with {row_id}")
        cursor.execute(query, (row_id,))
        cursor.fetchone()
        return True
    except Error as e:
        print(f"Error reading row {row_id}: {e}")
        return False
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def check_table_for_corrupted_rows(table_name):
    corrupted_rows = []
    min_id, max_id = fetch_id_range(table_name)
    if min_id is None or max_id is None:
        return

    for start_id in range(min_id, max_id + 1, 100):
        end_id = min(start_id + 99, max_id)
        if not check_id_range(table_name, start_id, end_id):
            for row_id in range(start_id, end_id + 1):
                if not check_individual_id(table_name, row_id):
                    log_corrupted_row(corrupted_rows, row_id, "Corrupted or inaccessible row")

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
