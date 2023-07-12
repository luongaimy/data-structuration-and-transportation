# Ai My Luong & Ronnel Martinez

import requests
import json
import os
from time import mktime
from datetime import date

def read_from_api(url, headers):
    response = requests.get(url, headers=headers)
    data = response.json()
    return json.dumps(data)  # Convert to string

def transform_data(data):
    data_json = json.loads(data)
    top_5 = data_json[:5]
    transformed_data = json.dumps(top_5)
    return transformed_data

def write_to_json(data, output_file):
    output_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(output_dir, output_file)
    
    with open(output_path, "w") as file:
        file.write(data)

def dts(input_date: str) -> int:
    return int(mktime(date.fromisoformat(input_date).timetuple()))
