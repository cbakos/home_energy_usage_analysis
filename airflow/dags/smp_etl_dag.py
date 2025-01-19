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
    dag_id="smp_etl_dag_v06",
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

    @task(task_id="get_meter_connections")
    def get_meter_connections():
        # Use HttpHook to interact with the connection
        http_hook = HttpHook(http_conn_id='smp_api_conn', method='GET')
        response = http_hook.run()

        # Process and log the response
        response_data = response.json()
        print(f"Meter connections: {response_data}")
        return response_data[0]  # Pass data to the next task if needed

    @task()
    def load_meter_connections_to_pg(data: Dict):
        hook = PostgresHook(postgres_conn_id='home_energy_pg_conn')

        hook.run(sql="sql/insert_smp_meter_connections.sql", parameters=data)


    create_smp_energy_usage_tables_task >> get_meter_connections()
    meter_data = get_meter_connections()
    load_meter_connections_to_pg(data=meter_data)


dag = etl_smp_data()