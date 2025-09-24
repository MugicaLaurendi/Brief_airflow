from google.transit import gtfs_realtime_pb2
import pandas as pd
import requests
import os
import pickle
import duckdb
from datetime import datetime as dt
from datetime import timedelta
from numpy import nan, isnan

def download_data_tu(DATA_DIR,PB_FILE):
    url = "https://ara-api.enroute.mobi/rla/gtfs/trip-updates"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(PB_FILE, "wb") as f:
        f.write(response.content)

    print(f"--- Donnees trips_update telechargees - {PB_FILE }")


def parse_data_tu(PB_FILE,PARSED_FILE):
    feed = gtfs_realtime_pb2.FeedMessage()

    with open(PB_FILE, "rb") as f:
        feed.ParseFromString(f.read())

    trips_update_list = []

    for entity in feed.entity:
        if entity.HasField("trip_update"):
            trip = entity.trip_update.trip

            trip_id = trip.trip_id
            route_id = trip.route_id
            direction_id = trip.direction_id if trip.HasField("direction_id") else None

            for stop_time_update in entity.trip_update.stop_time_update:
                stop_id = stop_time_update.stop_id
                stop_sequence = stop_time_update.stop_sequence

                if stop_time_update.HasField(
                    "arrival"
                ) and stop_time_update.arrival.HasField("time"):
                    arrival_time = stop_time_update.arrival.time
                else:
                    arrival_time = None

                if stop_time_update.HasField(
                    "departure"
                ) and stop_time_update.departure.HasField("time"):
                    departure_time = stop_time_update.departure.time
                else:
                    departure_time = None

                trips_update_list.append(
                    {
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "direction_id": direction_id,
                        "stop_id": stop_id,
                        "stop_sequence": stop_sequence,
                        "arrival_time": dt.fromtimestamp(arrival_time).time() if arrival_time else None,
                        "arrival_date": dt.fromtimestamp(arrival_time).date() if arrival_time else None,
                        "departure_time": dt.fromtimestamp(departure_time).time() if departure_time else None,
                        "departure_date":  dt.fromtimestamp(departure_time).date() if departure_time else None,
                    }
                )

    with open(PARSED_FILE, "wb") as f:
        pickle.dump(trips_update_list, f)
    print(f"parsing ok{PARSED_FILE}")

def store_in_warehouse_tu(PARSED_FILE,WAREHOUSE_DIR):

    with open(PARSED_FILE, "rb") as f:
        trips_update_list = pickle.load(f)

    df = pd.DataFrame(trips_update_list)

    now = str(dt.now() + timedelta(hours=2)).replace(" ","_")

    df.to_csv(f"{WAREHOUSE_DIR}/trip_update_{now}.csv")

    print(f"#### csv created and stored in : {WAREHOUSE_DIR}/trip_update_{now}.csv")

def export_data_to_duckdb_tu(PARSED_FILE,DUCK_DATABASE):
    with open(PARSED_FILE, "rb") as f:
        trips_update_list = pickle.load(f)

    df = pd.DataFrame(trips_update_list)

    con = duckdb.connect(DUCK_DATABASE)

    con.sql("CREATE TABLE trip_update AS SELECT * FROM df")

    con.sql("INSERT INTO trip_update SELECT * FROM df")
    
    print("--------------- RETURN SELECT ------------------------")
    con.sql("SELECT * FROM trip_update").show()
