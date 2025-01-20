import datetime
from typing import Dict, List

import pendulum
from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="smp_etl_dag_v18",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 10, tz="UTC"),
    end_date=pendulum.today().subtract(days=1),  # only up until yesterday to avoid empty data retrievals
    catchup=True,
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
        meter_connections = response.json()
        print(f"Meter connections received from API: {meter_connections}")
        return meter_connections

    @task()
    def load_meter_connections_to_pg(meter_connections):

        upsert_query = """
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
                for record in meter_connections
            ]

            # Execute the query for all records in the list
            cursor.executemany(upsert_query, prepared_data)
            conn.commit()
            print("Batch insert or update of meter connections is successful.")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

        return meter_connections

    @task()
    def get_usage_data_for_meter(meter_connection_details, **kwargs):
        # get date and meter_id to for usage endpoint
        date = kwargs['execution_date'].format("DD-MM-YYYY")
        print(date)
        meter_id = meter_connection_details["meter_identifier"]

        # Use HttpHook to retrieve usage date for meter on selected date
        http_hook = HttpHook(http_conn_id='smp_api_conn', method='GET')
        endpoint = f"/{meter_id}/usage/{date}"
        response = http_hook.run(endpoint=endpoint)

        # Process and log the response
        meter_readings = response.json()

        # add connection details for downstream processing
        meter_details_and_readings = meter_readings | meter_connection_details
        print(f"Meter readings received from API, including meter details: {meter_details_and_readings}")

        return meter_details_and_readings


    @task()
    def map_usage_to_data_schema(meter_details_and_readings) -> List[Dict]:
        print(meter_details_and_readings)
        all_mapped_usages = []
        for usage in meter_details_and_readings["usages"]:
            mapped_usages = []
            if meter_details_and_readings["connection_type"] == "gas":
                mapped_usage = {"time": usage["time"],
                                "reading_time_type": None,
                                "reading_subtype": "delivery",
                                "value": usage["delivery"],
                                "cumulative_reading": usage["delivery_reading"],
                                "temperature": usage["temperature"]}
                mapped_usages.append(mapped_usage)
            elif meter_details_and_readings["connection_type"]  == "elektriciteit":
                if usage["delivery_high"] is None:
                    is_reading_time_type_high = False
                elif usage["delivery_low"] is None:
                    is_reading_time_type_high = True
                else:
                    raise ValueError("None parsing failed...")
                mapped_usage_delivery = {"time": usage["time"],
                                         "reading_time_type": "high" if is_reading_time_type_high else "low",
                                         "reading_subtype": "delivery",
                                         "value": usage["delivery_high"] if is_reading_time_type_high else usage[
                                             "delivery_low"],
                                         "cumulative_reading": usage["delivery_reading_combined"],
                                         "temperature": usage["temperature"]}
                mapped_usage_return = {"time": usage["time"],
                                       "reading_time_type": "high" if is_reading_time_type_high else "low",
                                       "reading_subtype": "return",
                                       "value": usage["returned_delivery_high"] if is_reading_time_type_high else usage[
                                           "returned_delivery_low"],
                                       "cumulative_reading": usage["returned_delivery_reading_combined"],
                                       "temperature": usage["temperature"]}
                mapped_usages += [mapped_usage_delivery, mapped_usage_return]
            else:
                raise NotImplemented("Connection type not implemented.")
            all_mapped_usages += mapped_usages
        return all_mapped_usages


    meter_connections_data = get_meter_connections()
    create_smp_energy_usage_tables_task >> load_meter_connections_to_pg(meter_connections=meter_connections_data)
    meter_details_and_readings_list = get_usage_data_for_meter.expand(meter_connection_details=meter_connections_data)
    map_usage_to_data_schema.expand(meter_details_and_readings=meter_details_and_readings_list)


dag = etl_smp_data()