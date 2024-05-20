import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import json

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

def find_corrupted_ranges(table_name):
    corrupted_ranges = []
    min_id, max_id = fetch_id_range(table_name)
    if min_id is None or max_id is None:
        return

    for start_id in range(min_id, max_id + 1, 100):
        end_id = min(start_id + 99, max_id)
        if not check_id_range(table_name, start_id, end_id):
            corrupted_ranges.append((start_id, end_id))

    if corrupted_ranges:
        print(f"Corrupted ID ranges in {table_name}:")
        for start_id, end_id in corrupted_ranges:
            print(f"{start_id} to {end_id}")
    else:
        print(f"No corrupted ID ranges found in {table_name}")

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
        print("2. Find Corrupted ID Ranges")
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
            table_name = input("Enter the table name to check for corrupted ID ranges: ")
            find_corrupted_ranges(table_name)

        elif choice == '3':
            print("Exiting the program.")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
