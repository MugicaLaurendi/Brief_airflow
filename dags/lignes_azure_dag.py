from airflow import DAG
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import os

from lignes_azure.manage_database import init_database, remove_database

from lignes_azure.gtfs_reseau_transport import download_data_rt, export_data_to_duckdb_rt
from lignes_azure.gtfs_rt_trip_update import download_data_tu, parse_data_tu, export_data_to_duckdb_tu, store_in_warehouse_tu
from lignes_azure.gtfs_rt_vehicle_positions import download_data_vp, parse_data_vp, export_data_to_duckdb_vp, store_in_warehouse_vp

from lignes_azure.transformations import transformations


DATA_DIR = "/opt/airflow/data"
WAREHOUSE_DIR = "/opt/airflow/warehouse"
EXPORTS_DIR = "/opt/airflow/exports"

DUCK_DATABASE = os.path.join(DATA_DIR, "duck.db")

PB_FILE_RT = os.path.join(DATA_DIR, "reseau_transport.pb")

PB_FILE_TU = os.path.join(DATA_DIR, "trip_updates.pb")
PARSED_FILE_TU = os.path.join(DATA_DIR, "trip_updates_parsed.pkl")

PB_FILE_VP = os.path.join(DATA_DIR, "vehicle_positions.pb")
PARSED_FILE_VP = os.path.join(DATA_DIR, "vehicle_positions_parsed.pkl")


def init_database_call() : return init_database(DUCK_DATABASE)
def remove_database_call() : return remove_database(DUCK_DATABASE)

def download_reseau_transport_call() : return download_data_rt(DATA_DIR, PB_FILE_RT)
def duckdb_reseau_transport_call(): return export_data_to_duckdb_rt(PB_FILE_RT,DUCK_DATABASE,WAREHOUSE_DIR)

def download_trip_updates_call() : return download_data_tu(DATA_DIR, PB_FILE_TU)
def parse_trip_updates_call(): return parse_data_tu(PB_FILE_TU, PARSED_FILE_TU)
def duckdb_trip_updates_call(): return export_data_to_duckdb_tu(PARSED_FILE_TU,DUCK_DATABASE)
def store_in_warehouse_trip_updates_call(): return store_in_warehouse_tu(PARSED_FILE_TU,WAREHOUSE_DIR)

def download_vehicle_positions_call() : return download_data_vp(DATA_DIR, PB_FILE_VP)
def parse_vehicle_positions_call(): return parse_data_vp(PB_FILE_VP, PARSED_FILE_VP)
def duckdb_vehicle_positions_call(): return export_data_to_duckdb_vp(PARSED_FILE_VP,DUCK_DATABASE)
def store_in_warehouse_vehicle_positions_call(): return store_in_warehouse_vp(PARSED_FILE_VP,WAREHOUSE_DIR)

def transformations_call() : return transformations(DUCK_DATABASE,EXPORTS_DIR)


with DAG(

    dag_id="lignes_azure_ETL",
    tags=["lignes_azur"],
    start_date=datetime(2025, 9, 1),
    #schedule="0 8,13 * * *",  # 8h00 et 20h00 tous les jours
    catchup=False,
    default_args={"retries": 2},


) as dag:
        
    task_init_duckdb = PythonOperator(
        task_id="init_duckdb",
        python_callable= init_database_call,
    )


    with TaskGroup("reseau_transport") as TG_reseau_transport:
        
        task_download_reseau_transport = PythonOperator(
            task_id="download_reseau_transport",
            python_callable=download_reseau_transport_call,
        )

        task_download_reseau_transport


    with TaskGroup("trips_updates") as TG_trips_updates:
        
        task_download_trip_updates = PythonOperator(
            task_id="download_trip_updates",
            python_callable=download_trip_updates_call,
        )
        task_parse_trip_updates = PythonOperator(
            task_id="parse_trip_updates",
            python_callable=parse_trip_updates_call,
        )
        task_store_in_warehouse_trip_updates = PythonOperator(
            task_id="store_in_warehouse_trip_updates",
            python_callable=store_in_warehouse_trip_updates_call,
        )

        task_download_trip_updates >> task_parse_trip_updates >> task_store_in_warehouse_trip_updates


    with TaskGroup("vehicle_positions") as TG_vehicle_positions:
        
        task_download_vehicle_positions = PythonOperator(
            task_id="download_vehicle_positions",
            python_callable=download_vehicle_positions_call,
        )
        task_parse_vehicle_positions = PythonOperator(
            task_id="parse_vehicle_positions",
            python_callable=parse_vehicle_positions_call,
        )
        task_store_in_warehouse_vehicle_positions = PythonOperator(
            task_id="store_in_warehouse_vehicle_positions",
            python_callable=store_in_warehouse_vehicle_positions_call,
        )

        task_download_vehicle_positions >> task_parse_vehicle_positions >> task_store_in_warehouse_vehicle_positions

    with TaskGroup("load_duckdb") as TG_load_duckdb:

        task_duckdb_reseau_transport = PythonOperator(
            task_id="to_duckdb_reseau_transport",
            python_callable=duckdb_reseau_transport_call,
        )

        task_duckdb_trip_updates = PythonOperator(
            task_id="to_duckdb_trips_updates",
            python_callable=duckdb_trip_updates_call,
        )
        
        task_duckdb_vehicle_positions = PythonOperator(
            task_id="to_duckdb_vehicle_positions",
            python_callable=duckdb_vehicle_positions_call,
        )

        task_duckdb_reseau_transport >> task_duckdb_trip_updates >> task_duckdb_vehicle_positions

    with TaskGroup("duckdb_transformation") as TG_duckdb_transformation:
        
        task_duckdb = PythonOperator(
            task_id="duckdb_transformation",
            python_callable=transformations_call,
        )

        task_duckdb


    task_remove_duckdb = PythonOperator(
        task_id="remove_duckdb",
        python_callable= remove_database_call,
    )




    [TG_trips_updates,TG_vehicle_positions, TG_reseau_transport] >> task_init_duckdb >> TG_load_duckdb >> TG_duckdb_transformation >> task_remove_duckdb