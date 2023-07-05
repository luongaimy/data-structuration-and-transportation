from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
import requests
import json
from datetime import datetime
from time import mktime
from pytz import timezone
import os

def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(datetime.fromisoformat(input_date).timetuple()))

@dag(default_args={"owner": "airflow"}, schedule_interval=None, start_date=datetime(2022, 12, 1, tzinfo=timezone("UTC")))
def opensky_data_dag():
    BASE_URL = "https://opensky-network.org/api"
    
    @task
    def retrieve_flight_data():
        params = {
            "airport": "LFPG",  # ICAO code for CDG
            "begin": to_seconds_since_epoch("2022-12-01"),
            "end": to_seconds_since_epoch("2022-12-02")
        }
        
        cdg_flights = f"{BASE_URL}/flights/departure"
        response = requests.get(cdg_flights, params=params)
        flights = response.json()
        flights_string = json.dumps(flights)  # Convert to string
        return flights_string
    
    @task
    def print_flight_data(flights_string):
        print(flights_string)  # Replace with appropriate task to store or process the data
    
    @task
    def write_to_json(flights_string):
        output_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = "flight_data.json"
    
        output_path = os.path.join(output_dir, output_file)
    
        with open(output_path, "w") as file:
            file.write(flights_string)
    
        return output_path

    flights_data = retrieve_flight_data()
    print_flight_data(flights_data)
    write_task = write_to_json(flights_data)

dag = opensky_data_dag()