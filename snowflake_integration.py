import snowflake.connector
import csv
from details import account, user, password, database, schema


def connect_to_snowflake():
    # Snowflake connection details
    snowflake_account = account
    snowflake_user = user
    snowflake_password = password
    snowflake_database = database
    snowflake_schema = schema

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_account,
        user=snowflake_user,
        password=snowflake_password,
        database=snowflake_database,
        schema=snowflake_schema
    )
    return conn

def create_table(conn, table_name):
    # Create the table with the specified columns
    create_table_sql = f"""
    CREATE TABLE {table_name} (
        username STRING,
        datetime STRING,
        tweet_text STRING,
        replies INT,
        retweets INT,
        likes INT
    )
    """
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    cursor.close()

def stage_data(conn, table_name, data):
    # Export data to CSV
    csv_filename = f"{table_name}_data.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["username", "datetime", "tweet_text", "replies", "retweets", "likes"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in data:
            writer.writerow(item)

    # Create or replace the stage
    stage_sql = f"""
    CREATE OR REPLACE STAGE {table_name}_stage
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
    """
    cursor = conn.cursor()
    cursor.execute(stage_sql)
    cursor.close()

    # Stage the data in Snowflake
    put_sql = f"""
    PUT file://{csv_filename} @{table_name}_stage
    """
    cursor = conn.cursor()
    cursor.execute(put_sql)
    cursor.close()

def load_data(conn, table_name):
    # Load data from the staged file into the table
    copy_into_sql = f"""
    COPY INTO {table_name}
    FROM @{table_name}_stage/{table_name}_data.csv
    FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)
    ON_ERROR = CONTINUE  -- Ignore errors and continue loading
    """
    cursor = conn.cursor()
    try:
        cursor.execute(copy_into_sql)
    except snowflake.connector.errors.ProgrammingError as e:
        # Handle the error (optional)
        print("Error occurred during data loading:", str(e))
    finally:
        cursor.close()


