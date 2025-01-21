import datetime
from typing import Dict, List

import pendulum
from airflow.decorators import task, dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="smp_etl_dag_v39",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 7, 29, tz="UTC"),
    end_date=pendulum.today().subtract(days=2),  # only up until day before yesterday to avoid empty data retrievals
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    default_args={
        "retries": 2,  # Retry failed tasks twice
        "retry_delay": datetime.timedelta(days=1),  # Delay between retries
    },
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
            raise  # ensure airflow task fails if error occurs

        finally:
            cursor.close()
            conn.close()

        return prepared_data

    @task()
    def get_usage_data_for_meter(meter_connection_details, **kwargs):
        # get date and meter_id to for usage endpoint
        date = kwargs['execution_date'].format("DD-MM-YYYY")
        meter_id = meter_connection_details["meter_identifier"]

        # Use HttpHook to retrieve usage date for meter on selected date
        http_hook = HttpHook(http_conn_id='smp_api_conn', method='GET')
        endpoint = f"/{meter_id}/usage/{date}"
        response = http_hook.run(endpoint=endpoint)

        # Process and log the response
        meter_readings = response.json()

        # if no usages found, raise exception to ensure the task fails
        if not meter_readings["usages"]:
            raise Exception(f"No usages found for meter {meter_id} on {date}")

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
                mapped_usage = {"meter_identifier": meter_details_and_readings["meter_identifier"],
                                "time": usage["time"],
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
                mapped_usage_delivery = {"meter_identifier": meter_details_and_readings["meter_identifier"],
                                         "time": usage["time"],
                                         "reading_time_type": "high" if is_reading_time_type_high else "low",
                                         "reading_subtype": "delivery",
                                         "value": usage["delivery_high"] if is_reading_time_type_high else usage[
                                             "delivery_low"],
                                         "cumulative_reading": usage["delivery_reading_combined"],
                                         "temperature": usage["temperature"]}
                mapped_usage_return = {"meter_identifier": meter_details_and_readings["meter_identifier"],
                                       "time": usage["time"],
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

    @task()
    def clean_mapped_usage_entries(mapped_usages: List):
        import itertools, pandas as pd
        pd.set_option('display.max_columns', None)
        combined = list(itertools.chain(*mapped_usages))
        df = pd.json_normalize(combined)

        # Ensure 'meter_identifier' is a string
        df['meter_identifier'] = df['meter_identifier'].astype(str)

        # Convert 'time' to datetime with timezone (if not already in that format)
        # Handle invalid dates by setting them as NaT by 'coerce'
        # Raise an error if any missing values are found in the 'time' column
        if pd.isna(df['time']).any():
            raise ValueError("Missing values encountered in 'time' column")

        # Ensure 'reading_time_type' is a string, with None where missing
        df['reading_time_type'] = df['reading_time_type'].where(df['reading_time_type'].isna(),
                                                                df['reading_time_type'].astype(str))
        # Ensure 'reading_subtype' is a non-null string as it's part of primary key
        df['reading_subtype'] = df['reading_subtype'].astype(str, errors='raise')

        # Convert 'value', 'cumulative_reading', 'temperature' to floats, set errors as NaN
        df['value'] = df['value'].fillna('').str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['value'] = pd.to_numeric(df['value'], errors='coerce', downcast='float')  # Coerce invalid entries to NaN

        df['cumulative_reading'] = df['cumulative_reading'].fillna('').str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['cumulative_reading'] = pd.to_numeric(df['cumulative_reading'], errors='coerce', downcast='float')

        df['temperature'] = df['temperature'].fillna('').str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce', downcast='float')

        cleaned_usage_entries = df.to_dict(orient='records')
        return cleaned_usage_entries

    @task()
    def load_mapped_usage_entries_to_pg(combined_mapped_usages: List):
        # Connect to PostgreSQL using PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='home_energy_pg_conn')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        # Define the SQL query for upsert
        insert_query = """
                INSERT INTO usage_entries (
                    meter_identifier, 
                    time, 
                    reading_subtype, 
                    reading_time_type, 
                    value, 
                    cumulative_reading, 
                    temperature
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (meter_identifier, time, reading_subtype)
                DO UPDATE SET 
                    reading_time_type = EXCLUDED.reading_time_type,
                    value = EXCLUDED.value,
                    cumulative_reading = EXCLUDED.cumulative_reading,
                    temperature = EXCLUDED.temperature;
            """

        try:
            # Prepare data in the correct format for executemany
            prepared_data = [
                (
                    entry['meter_identifier'],
                    pendulum.from_format(entry["time"], "DD-MM-YYYY HH:mm:ss Z").to_iso8601_string(),
                    entry['reading_subtype'],
                    entry['reading_time_type'],
                    entry['value'],
                    entry['cumulative_reading'],
                    entry['temperature'],
                )
                for entry in combined_mapped_usages
            ]
            # Use executemany to insert the data in bulk
            cursor.executemany(insert_query, prepared_data)
            conn.commit()
            print("Batch insert successful!")

        except Exception as e:
            print(f"An error occurred: {e}")
            conn.rollback()
            raise  # ensure airflow task fails if error occurs

        finally:
            cursor.close()
            conn.close()

        return combined_mapped_usages

    meter_connections_data = get_meter_connections()
    inserted_data = load_meter_connections_to_pg(meter_connections=meter_connections_data)
    meter_details_and_readings_list = get_usage_data_for_meter.expand(meter_connection_details=meter_connections_data)
    mapped_usages_list = map_usage_to_data_schema.expand(meter_details_and_readings=meter_details_and_readings_list)
    cleaned_combined_usages = clean_mapped_usage_entries(mapped_usages=mapped_usages_list)
    final_mapped_usages = load_mapped_usage_entries_to_pg(combined_mapped_usages=cleaned_combined_usages)

    # ensure we load usage entries only after meter connections are in the db, which is preceded by setting up the tables
    final_mapped_usages << inserted_data << create_smp_energy_usage_tables_task

dag = etl_smp_data()