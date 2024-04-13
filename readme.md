##  About the project

This project simulates the daily data loading process from various sources into a data warehouse to support data analysis tasks. The data flow involves connecting to data sources via `APIs`, retrieving the necessary data, and loading it into a staging schema within a `PostgreSQL` database. Once all data is loaded into the database, transformations are performed to create datamarts including the **customer retention** datamart. This datamart enables the analysis of customer retention rates across different product categories, _helping to identify the categories with the best customer retention_.

##  Key Technologies

* `PostgreSQL:` The project utilizes a PostgreSQL database for storing the staging data and datamarts.
* `Apache Airflow:` Airflow orchestrates the data pipeline, handling the scheduling and execution of data retrieval, transformation, and loading tasks.
* `Docker:` The project is set up to run within Docker containers for easier management, deployment, and scalability.

## Project Structure

The project is organized as follows:

* `.env:` Airflow configuration file for setting up the environment.
* `docker-compose.yaml:` Docker configuration file and resources for setting up the environment.
* `db_init.sql:` Contains codes to initialize and  create the database objects.
* `/dags:` Contains Apache Airflow DAG for orchestrating the data pipeline.
* `/dags/sql:` Contains the source code for data retrieval, transformation, and loading tasks.

##   Setting up environment

1. Setting the AIRFLOW_UID to the current user's ID for proper permissions handling within the Airflow environment:
```bash 
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. This command starts the _airflow-init_ services defined in the **docker-compose.yaml** file  This service is responsible for initializing Apache Airflow, such as setting up its database schema and other initial configuration tasks. 
```bash
docker-compose up airflow-init
```
If it did not create database schema correctly, you could create it manually. Get the container ID of the PostgreSQL service

```bash
docker ps
```
and create the database:

```bash
docker exec -it [postgresql container id] createdb -U airflow airflow
```
3. Start all the services defined in the **docker-compose.yaml** file and keep them running. This includes running the PostgreSQL database, the Apache Airflow scheduler, webserver, worker, and flower.
```bash
docker-compose up -d
```

4. To create all the database objects run following command modifying `-c` parameter values to the required SQL command:

```bash
docker exec -t [postgresql container id] psql -U airflow -c "CREATE SCHEMA staging;"
```

#### Creating API and database connection in Airflow:


| API                                                                 | Database                                                             |
|---------------------------------------------------------------------|----------------------------------------------------------------------|
| **Connection Id:** `http_conn_id`                                   | **Connection Id:** `postgresql_de`                                   |
| **Connection Type:** `HTTP`                                         | **Connection Type:** `Postgres`                                      |
| **Host:** `https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net`      | **Host:** `postgres`                                                 |
| **Extra:** `{ "api_key": "5f55e6c0-e9e5-4a9c-b313-63c01fc31460" }`  | **Database:** `airflow`                                              |
|                                                                     | **Password:** `airflow`                                              |
|                                                                     | **Port:** `5432`                                                     |


To check if connection to the database from airflow is established correctly connect to CLI of airflow:
```bash
docker exec -it [airflow scheduler container id] /bin/bash
```
and run following command:
```bash
nc -vz postgres 5432
```

That's it. Now, can connect to the Airflow webserver at `localhost:8080` and see the dag running.

