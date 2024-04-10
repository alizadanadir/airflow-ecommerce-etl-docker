import requests
import json
import time
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.http_hook import HttpHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

headers = {
    'X-Nickname': "aa-tolmachev",
    'X-Cohort': "1",
    'X-Project': 'True',
    'X-API-KEY': "5f55e6c0-e9e5-4a9c-b313-63c01fc31460",
    'Content-Type': 'application/x-www-form-urlencoded'
}

# base_url = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

def get_task(ti):
    print('Making request to get task_id')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')

def get_report(ti):

    task_id = ti.xcom_pull(key = 'task_id')
    while True:
        print('Making request to get report_id')
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()

        data = json.loads(response.content)
        if data["status"] == "SUCCESS":
            report_id = data["data"]["report_id"]
            break
        else:
            print('Still running. Trying again in 10 seconds')
            time.sleep(10)

    print(f'Report id: {report_id}')
    ti.xcom_push(key= 'report_id', value = report_id)

def get_increment(ti, date):
    print('Making request to get increment_id')
    report_id = ti.xcom_pull(key = 'report_id')

    response = requests.get(f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00', headers=headers)
    response.raise_for_status()

    data = json.loads(response.content)
    increment_id = data["data"]["increment_id"]
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')

    print(f'Increment id: {increment_id}')
    ti.xcom_push(key = 'increment_id', value = increment_id)

def upload_csv_to_db(ti, date, filename, db_schema, db_table, date_column):
    increment_id = ti.xcom_pull(key = 'increment_id')
    file = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{headers["X-Cohort"]}/{headers["X-Nickname"]}/project/{increment_id}/{filename}'
    print(file)
    local_file = date.replace('-', '') + '_' + filename
    print(local_file)
    response = requests.get(file)
    response.raise_for_status()
    open(f"{local_file}", "wb").write(response.content)
    print(response.content)

    df = pd.read_csv(local_file)

    if filename != 'customer_research_inc.csv':
        df=df.drop('id', axis=1)
        df=df.drop_duplicates(subset=['uniq_id'])

    if filename == 'user_order_log_inc.csv':
        if 'status' not in df.columns:
            df['status'] = 'shipped'

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    delete_query = f"DELETE FROM {db_schema}.{db_table} WHERE {date_column} = '{date}'"
    engine.execute(delete_query)

    row_count = df.to_sql(db_table, engine, schema=db_schema, if_exists='append', index=False)
    print(f'{row_count} rows was inserted')


args = {
    "owner": "nalizada",
    'email': ['nalizada@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'


dag = DAG(
    dag_id = 'ecommerce_dag',
    default_args=args,
    catchup = True,
    start_date=datetime.today() - timedelta(days=7),
    end_date =datetime.today()
)

get_task = PythonOperator(
    task_id = 'get_task',
    python_callable = get_task,
    dag = dag
)

get_report = PythonOperator(
    task_id = 'get_report',
    python_callable = get_report,
    dag = dag
)

get_increment = PythonOperator(
    task_id = 'get_increment',
    python_callable = get_increment,
    op_kwargs= {'date': business_dt},
    dag = dag
)

upload_user_order_log_to_db = PythonOperator(
    task_id = 'upload_user_order_log_to_db',
    python_callable = upload_csv_to_db,
    op_kwargs= {'date': business_dt,
                'filename':'user_order_log_inc.csv', 
                'db_schema': 'staging', 
                'db_table': 'user_order_log', 
                'date_column' : 'date_time'
                },
    dag = dag
)

upload_customer_research_to_db = PythonOperator(
    task_id = 'upload_customer_research_to_db',
    python_callable = upload_csv_to_db,
    op_kwargs = {'date': business_dt,
                'filename':'customer_research_inc.csv', 
                'db_schema': 'staging', 
                'db_table': 'customer_research', 
                'date_column' : 'date_id'
    }
)

upload_user_activity_log_to_db = PythonOperator(
    task_id = 'upload_user_activity_log_to_db',
    python_callable = upload_csv_to_db,
    op_kwargs = {'date': business_dt,
                'filename':'user_activity_log_inc.csv', 
                'db_schema': 'staging', 
                'db_table': 'user_activity_log', 
                'date_column' : 'date_time'
    }
)

with TaskGroup("update_d_tables", dag = dag) as d_tables:
    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/d_city.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/d_customer.sql")

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/d_item.sql")
    

with TaskGroup("update_f_tables", dag = dag) as f_tables:
    update_f_research_table = PostgresOperator(
        task_id='update_f_research',
        postgres_conn_id=postgres_conn_id,
        sql="sql/f_research.sql")

    update_f_daily_activity_table = PostgresOperator(
        task_id='update_f_daily_activity',
        postgres_conn_id=postgres_conn_id,
        sql="sql/f_daily_activity.sql")

    update_f_daily_sales_table = PostgresOperator(
        task_id='update_f_daily_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/f_daily_sales.sql")

get_task >> get_report >> get_increment >> [upload_user_order_log_to_db, upload_customer_research_to_db, upload_user_activity_log_to_db] >> d_tables >> f_tables