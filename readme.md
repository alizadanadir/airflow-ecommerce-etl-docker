echo -e "AIRFLOW_UID=$(id -u)" > .env

docker-compose up airflow-init

docker-compose up

Sometimes it bugs and do not create db so need to connect to postgre and create it manually
docker exec -it 431c86961e36 createdb -U airflow airflow


docker exec -t 78b7e12b57b1 psql -U airflow -c "CREATE SCHEMA staging; CREATE SCHEMA warehouse;"

to cmd of airflow:

docker exec -it airflow-ecommerce-etl-docker_airflow-scheduler_1 /bin/bash
nc -vz postgres 5432 # check connection for db
