import duckdb
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta

def transformations(DUCK_DATABASE,EXPORTS_DIR):
    print("-------- START ------------")

    con = duckdb.connect(DUCK_DATABASE)

    now = str(dt.now() + timedelta(hours=2)).replace(" ","_")

    print("### QUESTION 01")

    sql_request = """
    WITH retard AS(
    SELECT trip_update.trip_id, trip_update.stop_id, trip_update.arrival_time,stop_times.arrival_time 
    AS theoric_arrival_time, date_diff('minutes',trip_update.arrival_time,theoric_arrival_time) AS retard
    FROM trip_update 
    INNER JOIN stop_times 
    ON trip_update.trip_id = stop_times.trip_id
    )
    SELECT trip_id, avg(retard)
    FROM retard
    GROUP BY trip_id
    """

    con.sql(sql_request).show()
    qt1 = con.sql(sql_request).df()
    qt1.to_csv(f"{EXPORTS_DIR}/question01_{now}.csv")
    print(f"### csv exported to : {EXPORTS_DIR}/question01_{now}.csv")


    print("### QUESTION 02")

    sql_request = """
    WITH retard AS(
    SELECT trip_update.trip_id, date_diff('minutes',trip_update.arrival_time,stop_times.arrival_time) AS retard
    FROM trip_update 
    INNER JOIN stop_times 
    ON trip_update.trip_id = stop_times.trip_id
    )
    SELECT vehicle_positions.trip_id, vehicle_id,latitude,longitude,avg(retard.retard)
    FROM vehicle_positions
    INNER JOIN retard
    ON vehicle_positions.trip_id = retard.trip_id
    GROUP BY vehicle_positions.trip_id, vehicle_id,latitude,longitude
    """

    con.sql(sql_request).show()
    qt2 = con.sql(sql_request).df()
    qt2.to_csv(f"{EXPORTS_DIR}/question02_{now}.csv")
    print(f"### csv exported to : {EXPORTS_DIR}/question02_{now}.csv")


    print("### QUESTION 03")

    sql_request = """
    WITH retard AS (
    SELECT trip_update.stop_id, date_diff('minutes',trip_update.arrival_time,stop_times.arrival_time) AS retard
    FROM trip_update 
    INNER JOIN stop_times 
    ON trip_update.trip_id = stop_times.trip_id
    )
    SELECT stops.stop_id,stops.stop_lat,stops.stop_lon,avg(retard.retard)
    FROM stops 
    INNER JOIN retard
    ON retard.stop_id = stops.stop_id
    GROUP BY stops.stop_id,stops.stop_lat,stops.stop_lon
    """

    con.sql(sql_request).show()
    qt3 = con.sql(sql_request).df()
    qt3.to_csv(f"{EXPORTS_DIR}/question03_{now}.csv")
    print(f"### csv exported to : {EXPORTS_DIR}/question03_{now}.csv")


    print("### QUESTION 04")

    sql_request = """
    WITH retard AS(
    SELECT trip_update.trip_id, trip_update.route_id, trip_update.stop_id, trip_update.arrival_time,stop_times.arrival_time 
    AS theoric_arrival_time, date_diff('minutes',trip_update.arrival_time,theoric_arrival_time) AS retard
    FROM trip_update 
    INNER JOIN stop_times 
    ON trip_update.trip_id = stop_times.trip_id
    )
    SELECT route_id, avg(retard) as moyenne_retards
    FROM retard
    GROUP BY route_id
    ORDER BY moyenne_retards
    """

    con.sql(sql_request).show()
    qt4 = con.sql(sql_request).df()
    qt4.to_csv(f"{EXPORTS_DIR}/question04_{now}.csv")
    print(f"### csv exported to : {EXPORTS_DIR}/question04_{now}.csv")

    print("### QUESTION 06")

    sql_request = """
    WITH retard AS(
    SELECT trip_update.trip_id, date_diff('minutes',trip_update.arrival_time,stop_times.arrival_time) AS retard
    FROM trip_update 
    INNER JOIN stop_times 
    ON trip_update.trip_id = stop_times.trip_id
    )
    SELECT vehicle_positions.trip_id, vehicle_id,retard.retard
    FROM vehicle_positions
    INNER JOIN retard
    ON vehicle_positions.trip_id = retard.trip_id
    """

    con.sql(sql_request).show()
    qt4 = con.sql(sql_request).df()
    qt4.to_csv(f"{EXPORTS_DIR}/question06_{now}.csv")
    print(f"### csv exported to : {EXPORTS_DIR}/question06_{now}.csv")

    print("--------  END  ------------")
