# Ai-My LUONG and Ronnel MARTINEZ

# reads data from the OpenSky API, for example all flights leaving CDG airport on 2022-12-01
# outputs it as a string

import requests
import json
from datetime import date
from time import mktime

def to_seconds_since_epoch(input_date: str) -> int:
  return int(mktime(date.fromisoformat(input_date).timetuple()))

BASE_URL = "https://opensky-network.org/api"

params = {
    "airport": "LFPG",  # ICAO code for CDG
    "begin": to_seconds_since_epoch("2022-12-01"),
    "end": to_seconds_since_epoch("2022-12-02")
}

cdg_flights = f"{BASE_URL}/flights/departure"

response = requests.get(cdg_flights, params=params)
flights = response.json()
flights_string = json.dumps(flights)  # Convert to string

print(flights_string)
