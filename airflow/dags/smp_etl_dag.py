import datetime
from typing import Dict

import pendulum
import os

import requests
from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from tempfile import NamedTemporaryFile

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_dag"


@dag(
    dag_id="smp_etl_dag_v11",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 17, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def etl_smp_data():
    create_smp_energy_usage_tables_task = SQLExecuteQueryOperator(
        task_id="create_smp_energy_usage_tables",
        sql="sql/create_smp_energy_usage_tables.sql",
        conn_id="home_energy_pg_conn"
    )

    @task(task_id="save_smp_meter_connections")
    def save_smp_meter_connections():
        # Use HttpHook to interact with the connection
        http_hook = HttpHook(http_conn_id='smp_api_conn', method='GET')
        response = http_hook.run()

        # Process and log the response
        response_data = response.json()
        print(f"Meter connections received from API: {response_data}")

        insert_query = """
            INSERT INTO meters (meter_identifier, connection_type, start_date, end_date)
            VALUES (%s, %s, to_date(%s, 'DD-MM-YYYY'), to_date(%s, 'DD-MM-YYYY'))
            ON CONFLICT (meter_identifier)
            DO UPDATE SET 
                connection_type = EXCLUDED.connection_type,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date;
        """

        hook = PostgresHook(postgres_conn_id='home_energy_pg_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Prepare the data as a list of tuples for batch execution
            prepared_data = [
                (
                    record['meter_identifier'],
                    record['connection_type'],
                    record['start_date'],
                    record['end_date'],
                )
                for record in response_data
            ]

            # Execute the query for all records in the list
            cursor.executemany(insert_query, prepared_data)
            conn.commit()
            print("Batch insert or update of meter connections is successful.")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

        return response_data  # Pass data to the next task if needed

    create_smp_energy_usage_tables_task >> save_smp_meter_connections()


dag = etl_smp_data()