import os
import shutil

import psycopg2

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient

pg_creds = {
    'host': '127.0.0.1'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': 'secret'
}


def postgres_dump(**kwargs):
    base_dir = "/home/user/data_db"
    process_date = kwargs['ds']

    remove_file(base_dir, process_date)
    create_directory(base_dir, process_date)

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with open(file=os.path.join('.', 'data', 'clients.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.clients TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join('.', 'data', 'orders.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.orders TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join('.', 'data', 'aisles.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.aisles TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join('.', 'data', 'departments.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.departments TO STDOUT WITH HEADER CSV', csv_file)

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')
    client.makedirs('/dshop')

    client.upload('/dshop/clients.csv', os.path.join(base_dir, process_date) + '/clients.csv')
    client.upload('/dshop/orders.csv', os.path.join(base_dir, process_date) + '/orders.csv')
    client.upload('/dshop/aisles.csv', os.path.join(base_dir, process_date) + '/aisles.csv')
    client.upload('/dshop/departments.csv', os.path.join(base_dir, process_date) + './departments.csv')

def create_directory(base_dir, process_date):
    directory = os.path.join(base_dir, process_date)
    try:
        os.makedirs(directory, exist_ok=True)
    except (FileNotFoundError, PermissionError) as err:
        print("Could not create directory " + base_dir)


def remove_file(base_dir, process_date):
    try:
        directory = os.path.join(base_dir, process_date)
        shutil.rmtree(directory)
    except (FileNotFoundError, PermissionError) as err:
        print("Could not remove file from " + directory)


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    'postgres_dag',
    description='postgres dag',
    schedule_interval='@daily',
    start_date=datetime(2021, 2, 23, 1, 0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='postgres_function',
    dag=dag,
    provide_context=True,
    python_callable=postgres_dump
)
