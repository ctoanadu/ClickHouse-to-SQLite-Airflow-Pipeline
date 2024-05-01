import datetime
import sqlite3

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator


def create_table():
    """
    Creates a 'tripdata' table in the Airflow database using SQLite.

    Reads SQL query from 'create_table.sql' file, executes it, and commits the changes.

    Note:
    - 'create_table.sql' file contains the SQL query to create the table.
    - The SQLite database file 'airflow.db' is located in the 'db' directory.

    Returns:
    None
    """
    try:
        destination_conn = sqlite3.connect("db/airflow.db")
        destination_cursor = destination_conn.cursor()

        with open("../query/create_table.sql", "r") as file:
            sql_query = file.read()
        destination_cursor.execute(sql_query)
        destination_conn.commit()
        destination_conn.close()
        print("tripdata table created")
    except Exception as e:
        print(f"An error occured: {e}")


def extract_data():
    """
    Queries an external ClickHouse database.

    Uses a specified SQL query stored in 'trip_data_query.sql' file to retrieve data.
    Establishes a connection to the ClickHouse database using the provided credentials.

    Returns:
    Query result from the ClickHouse database.
    """
    try:
        client = clickhouse_connect.get_client(
            host="github.demo.altinity.cloud",
            port=8443,
            username="demo",
            password="demo",
        )
        with open("../query/trip_data_query.sql", "r") as file:
            sql_query = file.read()
        return client.command(sql_query)
    except Exception as e:
        print(f"An error occured: {e}")


def insert_table():
    """
    Inserts data into tripdata table in the Airflow database using SQLite.

    Retrieves data from an external source using the 'extract_data()' function.
    Processes the data and formats it appropriately for insertion into the database table.
    Reads SQL query for insertion from the file 'insert_table.sql'.
    Inserts the retrieved data into the newly created table.

    Note:
    - The 'insert_table.sql' file contains the SQL query for inserting data into the table.

    Returns:
    None
    """
    try:
        destination_conn = sqlite3.connect("db/airflow.db")
        destination_cursor = destination_conn.cursor()

        query_result = extract_data()
        formatted_data = []

        # Process data and split if needed
        for item in query_result:
            if "\n" in item:
                formatted_data.extend(item.split("\n"))
            else:
                formatted_data.append(item)

        # Group data into sublists of 7 elements each
        grouped_data = [
            formatted_data[i : i + 7] for i in range(0, len(formatted_data), 7)
        ]

        with open("../query/insert_table.sql", "r") as file:
            sql_query = file.read()

        # Insert the retrieved data into the newly created tripdata table
        for row in grouped_data:
            destination_cursor.execute(sql_query, row)

        destination_conn.commit()
        destination_conn.close()
    except Exception as e:
        print(f"An error occured: {e}")


with DAG(
    dag_id="main_dag",
    schedule_interval="@hourly",
    default_args={
        "owner": "airflow",
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
        "start_date": datetime.datetime.utcnow(),
    },
    catchup=False,
) as f:

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    extract_data_task = PythonOperator(
        task_id="extract_data", python_callable=extract_data, provide_context=True
    )

    insert_table_task = PythonOperator(
        task_id="insert_table",
        python_callable=insert_table,
    )


create_table_task >> extract_data_task >> insert_table_task
