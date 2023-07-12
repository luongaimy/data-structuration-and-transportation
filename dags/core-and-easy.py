# # Ai-My Luong & Ronnel Martinez
# # Importing the necessary libraries
# # Core task and Easy task

# Core only

# Importing the necessary libraries
# from airflow import DAG
# from airflow.exceptions import AirflowException
# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# import requests
# import json
# from datetime import datetime, timedelta
# from time import mktime
# from pytz import timezone
# import os

# # Function to convert a date string to seconds since epoch
# def to_seconds_since_epoch(input_date: str) -> int:
#     return int(mktime(datetime.fromisoformat(input_date).timetuple()))

# # Definition of the Airflow DAG
# @dag(default_args={"owner": "airflow"}, schedule_interval=None, start_date=datetime(2022, 12, 1, tzinfo=timezone("UTC")))
# def opensky_data_dag():
#     BASE_URL = "https://opensky-network.org/api"
    
#     # First task to retrieve flight data from the OpenSky API
#     @task
#     def retrieve_flight_data():
#         params = {
#             "airport": "LFPG",  # ICAO code for CDG
#             "begin": to_seconds_since_epoch("2022-12-01"),
#             "end": to_seconds_since_epoch("2022-12-02")
#         }
        
#         cdg_flights = f"{BASE_URL}/flights/departure"
#         response = requests.get(cdg_flights, params=params)
#         flights = response.json()
#         flights_string = json.dumps(flights)  # Convert data to string
#         return flights_string
    
#     # Second task to print the flight data as string
#     @task
#     def print_flight_data(flights_string):
#         print(flights_string)  
    
#     # Third task to write the flight data to a JSON file
#     @task
#     def write_to_json(flights_string):
#         output_dir = os.path.dirname(os.path.abspath(__file__)) # Get the directory of the current script
#         output_file = "flight_data.json"
    
#         output_path = os.path.join(output_dir, output_file)
    
#         with open(output_path, "w") as file:
#             file.write(flights_string)
    
#         return output_path

#     flights_data = retrieve_flight_data() # Run the first original task
#     print_flight_data(flights_data) # Run the second original task
#     write_task = write_to_json(flights_data) # Run the third original task

# dag = opensky_data_dag()

# Core and Easy version

# Importing the necessary libraries
from typing import Dict, List  # Import typing for type hints
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import requests
import json
from datetime import datetime, timedelta
from time import mktime
from pytz import timezone
import os

# Base URL for OpenSky API
BASE_URL = "https://opensky-network.org/api"

# Function to convert a date string to seconds since epoch
# Input is a string, output is an integer
def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(datetime.fromisoformat(input_date).timetuple()))

# Definition of the Airflow DAG
# The start_date is the date when the DAG should start running
# The schedule_interval specifies that the DAG will run every day at 1 AM
@dag(default_args={"owner": "airflow", "catchup": False}, schedule_interval='0 1 * * *', start_date=days_ago(1))
def opensky_data_dag():
    
    # First task to retrieve flight data from the OpenSky API
    # The task returns a dictionary with multiple outputs
    @task(multiple_outputs=True)
    def retrieve_flight_data() -> Dict[str, List[Dict]]:
        params = {
            "airport": "LFPG",  # ICAO code for CDG
            "begin": to_seconds_since_epoch((datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')),
            "end": to_seconds_since_epoch(datetime.now().strftime('%Y-%m-%d'))
        }
        
        try:
            cdg_flights = f"{BASE_URL}/flights/departure"
            response = requests.get(cdg_flights, params=params)
            response.raise_for_status()  # Raises a HTTPError if one occurred
        except requests.exceptions.RequestException as err:
            raise AirflowException(f"API request failed due to {err}")
        
        flights = response.json()
        return {"flights": flights}  # Return a dictionary
    
    # Second task to print the flight data as a dictionary
    @task
    def print_flight_data(flights_data: Dict[str, List[Dict]]):
        print(flights_data)
    
    # Third task to write the flight data to a JSON file
    # This task now receives a dictionary with the flight data
    @task
    def write_to_json(flights_data: Dict[str, List[Dict]]):
        output_dir = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
        output_file = "flight_data.json"
        output_path = os.path.join(output_dir, output_file)
        
        try:
            with open(output_path, "w") as file:
                file.write(json.dumps(flights_data))  # Convert the dictionary to a string
        except Exception as err:
            raise AirflowException(f"File writing failed due to {err}")
        
        return output_path

    # Execution of tasks
    flights_data = retrieve_flight_data()  # Run the first task
    print_flight_data(flights_data)  # Run the second task
    write_to_json(flights_data)  # Run the third task

dag = opensky_data_dag()  # Define the DAG
