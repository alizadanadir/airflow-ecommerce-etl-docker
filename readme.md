echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

docker-compose up

Sometimes it bugs and do not create db so need to connect to postgre and create it manually
docker exec -it 431c86961e36 createdb -U airflow airflow


docker exec -t 78b7e12b57b1 psql -U airflow -c "CREATE SCHEMA staging; CREATE SCHEMA warehouse;"
