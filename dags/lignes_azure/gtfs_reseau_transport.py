import requests
import zipfile
import io
import os
import duckdb
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
from numpy import nan, isnan


def download_data_rt(DATA_DIR,PB_FILE):

    url = "https://chouette.enroute.mobi/api/v1/datas/OpendataRLA/gtfs.zip"
    response = requests.get(url)
    response.raise_for_status()

    os.makedirs(DATA_DIR, exist_ok=True)

    with open(PB_FILE, "wb") as f:
        f.write(response.content)

    print(f"--- Donnees reseau_transport telechargees - {PB_FILE }")

def export_data_to_duckdb_rt(PB_FILE,DUCK_DATABASE,WAREHOUSE_DIR):

    with open(PB_FILE, "rb") as f:
        response = f.read()
        
    files_not_wanted = ["agency.txt","feed_info.txt"]

    con = duckdb.connect(DUCK_DATABASE)

    with zipfile.ZipFile(io.BytesIO(response)) as z:
        for filename in z.namelist():
            if filename not in files_not_wanted:

                with z.open(filename) as f:
                    df = pd.read_csv(f)

                table_name = filename.replace(".txt","")

                print(f"---------- {table_name} -------------")

                match table_name :

                    case "calendar":
                    
                        df['start_date'] = df['start_date'].apply(lambda x :  dt.strptime(str(x), "%Y%m%d").date() if not isnan(x) else nan)
                        df['end_date'] = df['end_date'].apply(lambda x :  dt.strptime(str(x), "%Y%m%d").date() if not isnan(x) else nan)

                    case "stop_times":

                        df['arrival_time'] = df['arrival_time'].apply(lambda x :"00" + x[2:] if not pd.isnull(x) and x[:2] == "24" else x)
                        df['arrival_time'] = df['arrival_time'].apply(lambda x : dt.strptime(x, "%H:%M:%S").time() if not pd.isnull(x) else nan)

                        df['departure_time'] = df['departure_time'].apply(lambda x :"00" + x[2:] if not pd.isnull(x) and x[:2] == "24" else x)
                        df['departure_time'] = df['departure_time'].apply(lambda x : dt.strptime(x, "%H:%M:%S").time() if not  pd.isnull(x) else nan)

                    case "calendar_dates":
                    
                        df['date'] = df['date'].apply(lambda x :  dt.strptime(str(x), "%Y%m%d").date() if not isnan(x) else nan)


                now = str(dt.now() + timedelta(hours=2)).replace(" ","_")
                df.to_csv(f"{WAREHOUSE_DIR}/reseau_transport_static/{table_name}_{now}.csv")

                print(f"#### csv created and stored in : {WAREHOUSE_DIR}/reseau_transport_static/{table_name}_{now}.csv")


                con.sql(f"CREATE TABLE {table_name} AS SELECT * FROM df")

                con.sql(f"INSERT INTO {table_name} SELECT * FROM df")
                
                con.sql(f"SELECT * FROM {table_name}").show()
