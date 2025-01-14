import os
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

def get_meter_connections():
    url = f"{BASE_URL}"
    return send_get_request(url=url)

def get_meter_readings_per_day(meter_id: str, date: str):
    url = f"{BASE_URL}/{meter_id}/usage/{date}"
    return send_get_request(url=url)


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

    # Call the function to fetch meter readings
    meters_data = get_meter_connections()

