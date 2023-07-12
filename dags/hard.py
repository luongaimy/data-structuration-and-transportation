# Ai-My Luong & Ronnel Martinez
# Hard task
import sqlite3
from dataclasses import dataclass
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
import requests
from utils import dts

@dataclass
class FlightData:
  icao24: str
  firstSeen: int
  estDepartureAirport: str
  lastSeen: int
  estArrivalAirport: str
  callsign: str
  estDepartureAirportHorizDistance: int
  estDepartureAirportVertDistance: int
  estArrivalAirportHorizDistance: int
  estArrivalAirportVertDistance: int
  departureAirportCandidatesCount: int
  arrivalAirportCandidatesCount: int

@dag(
    start_date = datetime.now(),
    schedule_interval = "0 1 * * *",
    catchup = False
)
def FlightDataProcessing():
    API_BASE_URL = "https://opensky-network.org/api"

    @task
    def fetch_data(ds=None):
        ds = date.today() - timedelta(days=7)
        next_day = ds + timedelta(days=1)
        params = {
            "airport": "LFPG",
            "begin": dts(str(ds)),
            "end": dts(str(next_day))
        }
        response = requests.get(f"{API_BASE_URL}/flights/departure", params=params)
        flight_data = response.json()
        return flight_data

    @task
    def store_in_db(flight_data):
        with sqlite3.connect(":memory:") as conn:
            cursor = conn.cursor()
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS flight_data (
                icao24 TEXT,
                firstSeen INTEGER,
                estDepartureAirport TEXT,
                lastSeen INTEGER,
                estArrivalAirport TEXT,
                callsign TEXT,
                estDepartureAirportHorizDistance INTEGER,
                estDepartureAirportVertDistance INTEGER,
                estArrivalAirportHorizDistance INTEGER,
                estArrivalAirportVertDistance INTEGER,
                departureAirportCandidatesCount INTEGER,
                arrivalAirportCandidatesCount INTEGER
            )
            ''')

            for flight in flight_data:
                cursor.execute('''INSERT INTO flight_data (icao24, firstSeen, estDepartureAirport, lastSeen, estArrivalAirport, callsign, estDepartureAirportHorizDistance, estDepartureAirportVertDistance, estArrivalAirportHorizDistance, estArrivalAirportVertDistance, departureAirportCandidatesCount, arrivalAirportCandidatesCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',(flight['icao24'], flight['firstSeen'], flight['estDepartureAirport'], flight['lastSeen'], flight['estArrivalAirport'], flight['callsign'], flight['estDepartureAirportHorizDistance'], flight['estDepartureAirportVertDistance'], flight['estArrivalAirportHorizDistance'], flight['estArrivalAirportVertDistance'], flight['departureAirportCandidatesCount'], flight['arrivalAirportCandidatesCount']))
            conn.commit()

            cursor.execute("SELECT * FROM flight_data")
            rows = cursor.fetchall()

            for row in rows:
                print(row)

            cursor.execute('''DROP TABLE flight_data''')

    fetch = fetch_data()
    store = store_in_db(fetch)
    fetch >> store

_ = FlightDataProcessing()
