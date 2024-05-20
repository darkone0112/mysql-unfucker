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

def transfer_rows():
    connection = connect_to_db()
    cursor = connection.cursor()

    # Fetching the minimum and maximum IDs from the table
    min_id_query = "SELECT MIN(id) FROM computers"
    max_id_query = "SELECT MAX(id) FROM computers"
    
    cursor.execute(min_id_query)
    min_id = cursor.fetchone()[0]
    
    cursor.execute(max_id_query)
    max_id = cursor.fetchone()[0]
    
    # Processing from min to max until first corruption
    print("Processing from min to max...")
    last_successful_min_to_max = process_range(cursor, connection, min_id, max_id, 1)
    


    # Closing connection
    cursor.close()
    connection.close()
    print("MySQL connection closed")
    print(f"Last successful insert from min to max: {last_successful_min_to_max}")


def process_range(cursor, connection, start, end, step):
    last_successful_id = None
    try:
        for current_id in range(start, end + step, step):
            cursor.execute(f"""
                INSERT INTO backup_computers
                SELECT * FROM computers
                WHERE id = {current_id};
            """)
            connection.commit()
            last_successful_id = current_id
            print(f"Successfully transferred row with id {current_id}.")
    except Error as e:
        print(f"Error during row transfer for id {current_id}: {e}")
        if '2013' in str(e):
            print(f"Encountered a corrupted row at ID {current_id}.")
        else:
            print("Unexpected error occurred.")
    return last_successful_id

if __name__ == "__main__":
    transfer_rows()