# Ai-My Luong & Ronnel Martinez
# Medium task
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
from pytz import timezone
from utils import read_from_api, transform_data, write_to_json

@dag(default_args={"owner": "airflow"}, schedule_interval=None, start_date=datetime(2022, 12, 1, tzinfo=timezone("UTC")))
def med():
    BASE_URL = "https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid"
    PUUID = "zOUMB79QVMF1gHfFwDwuHlsRwMOR8OmQt3q8azwh-xOTBeRvx4bo9e-VDHaaQw-B8xp5z7vYn9KbyQ"
    API_KEY = "RGAPI-e8177e36-690f-4370-8f06-aae4b0f6bb0a"  # Change to your API key
    
    '''API Keys from RIOT games expire after 24 hours. Please change the value accordingly
    LOGIN HERE >>> https://developer.riotgames.com and create your account
    Go to your dashboard and generate your own API Key'''
    
    @task
    def retrieve_match_data():
        headers = {"X-Riot-Token": API_KEY}
        
        match_data = read_from_api(f"{BASE_URL}/{PUUID}/ids?start=0&count=20", headers)
        return match_data
    
    @task
    def transform_match_data(match_data):
        transformed_data = transform_data(match_data)  # Implement your transformation logic in transform_data function
        return transformed_data
    
    @task
    def print_match_data(match_data):
        print(match_data)  # Replace with appropriate task to store or process the data
    
    @task
    def write_to_json_task(match_data):
        output_file = "match_data.json"
        output_path = write_to_json(match_data, output_file)
        return output_path

    match_data = retrieve_match_data()
    transformed_data = transform_match_data(match_data)
    print_match_data(transformed_data)
    write_task = write_to_json_task(transformed_data)

dag = med()
