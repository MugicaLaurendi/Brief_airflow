Pour build l'image de Dockerfile :
docker build -t airflow .

Pour construire le docker compose :
docker compose up -d

Pour recuperer les doonnées :
docker cp airflow-gtfs-duckdb-airflow-apiserver-1:/opt/airflow/warehouse ./warehouse_import
