import os
from typing import List, Dict

import dlt
import requests
from dotenv import load_dotenv

def send_get_request(url: str):
    try:
        # Make the GET request
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad HTTP responses (4xx or 5xx)
        data = response.json()  # Parse the JSON response
        print("Response:", data)
        return data
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return -1

def get_meter_connections() -> List[Dict]:
    url = f"{BASE_URL}"
    return send_get_request(url=url)

def get_meter_readings_per_day(meter_id: str, date: str):
    url = f"{BASE_URL}/{meter_id}/usage/{date}"
    return send_get_request(url=url)

def map_usage_to_data_schema(usage: Dict, connection_type: str) -> List[Dict]:
    mapped_usages = []
    if connection_type == "gas":
        mapped_usage = {"time": usage["time"],
                        "reading_time_type": None,
                        "reading_subtype": "delivery",
                        "value": usage["delivery"],
                        "cumulative_reading": usage["delivery_reading"],
                        "temperature": usage["temperature"]}
        mapped_usages.append(mapped_usage)
    elif connection_type == "elektriciteit":
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
    return mapped_usages

@dlt.resource(name="combined_smp_data")
def combine_smp_data():
    meter_connections = get_meter_connections()
    meter_readings_per_connection = []
    for connection in meter_connections:
        meter_readings = get_meter_readings_per_day(meter_id=connection["meter_identifier"], date="10-01-2025")
        # merge dicts to have one that contains meter connection details and usage records too
        readings_and_meter_details = meter_readings | connection
        all_mapped_usages = []
        for usage in readings_and_meter_details["usages"]:
            all_mapped_usages += map_usage_to_data_schema(usage=usage, connection_type=connection["connection_type"])
        readings_and_meter_details["usages"] = all_mapped_usages
        meter_readings_per_connection.append(readings_and_meter_details)
    yield meter_readings_per_connection

@dlt.source()
def get_smp_data():
    return combine_smp_data()


if __name__ == '__main__':

    BASE_URL = "https://app.slimmemeterportal.nl/userapi/v1/connections"

    load_dotenv()

    # Retrieve the API key
    api_key = os.getenv("SMP_API_KEY")

    # Headers for the request
    HEADERS = {
        "API-Key": f"{api_key}",
        "accept": "application/json"
    }
    pipeline = dlt.pipeline(
        pipeline_name="smp_meter_connections",
        destination="duckdb",
        dataset_name="smp_data",
    )

    load_info = pipeline.run(get_smp_data())
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")
    print(load_info)
    print(pipeline.dataset())

    # Call the function to fetch meter readings
    # meters_data = get_meter_connections()

