import os
import requests
from dotenv import load_dotenv

def get_meter_connections():
    url = f"{BASE_URL}"
    try:
        # Make the GET request
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad HTTP responses (4xx or 5xx)
        data = response.json()  # Parse the JSON response
        print("Connections:", data)
        return data
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

def get_meter_readings(meter_id: str, date: str):
    url = f"{BASE_URL}/{meter_id}/usage/{date}"
    try:
        # Make the GET request
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Raise an error for bad HTTP responses (4xx or 5xx)
        data = response.json()  # Parse the JSON response
        print("Meter Readings:", data)
        return data
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

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
    
