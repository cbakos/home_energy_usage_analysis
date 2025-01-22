# Home Energy Usage Analysis

A personal project to gain insights into energy consumption of the house where I live, 
optimize consumption, predict possible savings by different improvements 
to the apartment.


## Part 1: Data Engineering

### 1.1: Data sources
- Electricity usage - smart meter
- Gas usage - smart meter
- Water usage (future extension, no smart meter available currently)
- Electricity market price
- Electricity price paid
- Gas market price
- Gas price paid
- Netatmo Energy - fine-grained heating and room temperature data from smart radiator valves
- Local weather data: 
  - temperature
  - sunshine/cloudiness (future extension for solar panels)

### 1.2 ETL pipelines
- electricity, gas usage and temperature via slimmemeterportal.nl API
  (hourly and 15 min interval data, available after 48 hours)



## Part 2: Data Analytics 
- analyse existing trends, dashboards

## Part 3: Data Science 
- predictive models

## Setup instructions
- be sure to have Docker and docker compose installed
- in the airflow folder make a copy of ```.env-template``` and rename your copy to 
```.env```. Choose your own username and password to be used for the postgres DB in the project.
- In ```airflow/dags/smp_etl_dag.py``` file, in the ```@dag(...)``` configuration set the start date
to the time when your smart meter was installed.
- Run ```docker compose up -d``` from the ```/airflow``` folder
- Navigate to ```http://localhost:8080/``` in your browser, login with username and password ```airflow```.
- In the admin/connections tab you need to setup two connections:
  1. Connection to your DB: 
  ```
  Connection Id: home_energy_pg_conn
  Connection Type: Postgres
  Host, DB: home_energy_db
  Username, Password: <use your values from the .env file>
  Port: 5432
  ```

  2. Connect to Slimmemeterportal.nl API
  ```
  Connection Id: smp_api_conn
  Connection Type: HTTP
  Host: https://app.slimmemeterportal.nl/userapi/v1/connections
  Schema: https
  Extra: {
  "API-Key": "<your api key>",
  "accept": "application/json"
  }
  ```
- Now you are ready to activate the ```smp_etl_dag```. 
This will extract all gas and electricity meter readings available 
and it will keep extracting it to your DB every day.
