from google.transit import gtfs_realtime_pb2
import pandas as pd
import requests
import os
import pickle
import duckdb
from datetime import datetime as dt
from datetime import timedelta


def download_data_vp(DATA_DIR,PB_FILE):
    url = "https://ara-api.enroute.mobi/rla/gtfs/vehicle-positions"
    response = requests.get(url)

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(PB_FILE, "wb") as f:
        f.write(response.content)

    print(f"--- Donnees vehicle_positions telechargees - {PB_FILE }")


def parse_data_vp(PB_FILE,PARSED_FILE):

    feed = gtfs_realtime_pb2.FeedMessage()

    with open(PB_FILE, "rb") as f:
        feed.ParseFromString(f.read())

    records = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        trip = vehicle.trip
        pos = vehicle.position

        record = {
            "trip_id": trip.trip_id if trip.HasField("trip_id") else None,
            "route_id": trip.route_id if trip.HasField("route_id") else None,
            "latitude": pos.latitude if pos.HasField("latitude") else None,
            "longitude": pos.longitude if pos.HasField("longitude") else None,
            "bearing": pos.bearing if pos.HasField("bearing") else None,
            "timestamp": dt.fromtimestamp(vehicle.timestamp) if vehicle.HasField("timestamp") else None,
            "stop_id": vehicle.stop_id if vehicle.HasField("stop_id") else None,
            "vehicle_id": vehicle.vehicle.id if vehicle.HasField("vehicle") else None,
        }
        records.append(record)

    with open(PARSED_FILE, "wb") as f:
        pickle.dump(records, f)
    print(f"parsing ok{PARSED_FILE}")

def store_in_warehouse_vp(PARSED_FILE,WAREHOUSE_DIR):

    with open(PARSED_FILE, "rb") as f:
        trips_update_list = pickle.load(f)

    df = pd.DataFrame(trips_update_list)

    now = str(dt.now() + timedelta(hours=2)).replace(" ","_")

    df.to_csv(f"{WAREHOUSE_DIR}/vehicle_positions_{now}.csv")

    print(f"#### csv created and stored in : {WAREHOUSE_DIR}/vehicle_positions_{now}.csv")


def export_data_to_duckdb_vp(PARSED_FILE,DUCK_DATABASE):
    with open(PARSED_FILE, "rb") as f:
        trips_update_list = pickle.load(f)

    df = pd.DataFrame(trips_update_list)

    con = duckdb.connect(DUCK_DATABASE)

    con.sql("CREATE TABLE vehicle_positions AS SELECT * FROM df")

    con.sql("INSERT INTO vehicle_positions SELECT * FROM df")
    
    print("--------------- RETURN SELECT ------------------------")
    con.sql("SELECT * FROM vehicle_positions").show()
