import pymysql
import pandas as pd
import time

# Database connection details
DB_CONFIG = {
    "host": "localhost",
    "user": "thisisauser",
    "password": "thisisapassword",
    "database": "RUST_TEST",
    "port": 3306  
}

# SQL query
QUERY = "SELECT * FROM RUST_TEST.salary_data"

# Output Parquet file path
OUTPUT_FILE = "py_salaray_data.parquet"

def query_mysql_and_save_to_parquet():
    """
    Queries data from a MySQL database and saves the result as a parquet file.

    The function connects to the MySQL database using the provided configuration,
    executes a query to retrieve data, and saves it as a parquet file using PyArrow.
    The function also measures the execution time for performance analysis.

    Raises:
        Exception: If there is an issue with the database connection, query execution,
                   or writing the parquet file.
    """
    # Start measuring total execution time
    start_time = time.time() 
    
    try:
        # Connect to MySQL
        conn = pymysql.connect(**DB_CONFIG)
        print("Connected to database.")

        # Start timing query execution
        query_start_time = time.time()
        df = pd.read_sql(QUERY, conn)
        query_end_time = time.time()
        print(f"Query execution time: {query_end_time - query_start_time:.2f} seconds")

        # Start timing Parquet writing
        write_start_time = time.time()
        df.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)
        write_end_time = time.time()
        print(f"Parquet writing time: {write_end_time - write_start_time:.2f} seconds")

    # Handle errors
    except Exception as e:
        print(f"Error: {e}")

    # Ensure that the connection to the databse is closed
    finally:
        conn.close()
        print("Database connection closed.")

    # Stop time - calculate total execution time
    end_time = time.time()  
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    query_mysql_and_save_to_parquet()
