import requests
import json
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient


def currency(**kwargs):
    try:
        process_date = kwargs['ds']
        print(f"process_date = {process_date}")

        base_dir = "/home/user/data"
        url = "https://robot-dreams-de-api.herokuapp.com/auth"
        r = requests.post(url, headers={"content-type": "application/json"},
                          data=json.dumps({"username": "rd_dreams", "password": "djT6LasE"}))
        token = r.json()['access_token']

        url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
        headers = {"content-type": "application/json", "Authorization": "JWT " + token}
        data = {"date": "2021-10-08"}
        responce = requests.get(url, headers=headers, data=json.dumps(data)).json()

        remove_file(base_dir, process_date, process_date)
        create_directory(base_dir, process_date)
        save_data(base_dir, responce, process_date)


        client = InsecureClient(f'http://127.0.0.1:50070/', user='user')
        pathToFile = os.path.join('/dshop', process_date)
        fileName = process_date + '.json'
        client.delete(pathToFile, recursive=False)
        client.makedirs(pathToFile)
        client.upload(pathToFile + fileName, os.path.join(base_dir, process_date) + fileName)

    except ConnectionError as err:
        print("Could not get data")


def create_directory(base_dir, dir_name):
    directory = os.path.join(base_dir, dir_name)
    try:
        os.makedirs(directory, exist_ok=True)
    except (FileNotFoundError, PermissionError) as err:
        print("Could not create directory " + base_dir + "/" + dir_name)


def remove_file(base_dir, dir_name, file_name):
    directory = os.path.join(base_dir, dir_name, file_name)
    try:
        os.remove(directory)
    except (FileNotFoundError, PermissionError) as err:
        print("Could not remove file " + base_dir + "/" + dir_name)


def save_data(base_dir, data_json, process_date):
    try:
        directory = os.path.join(base_dir, process_date)
        with open(os.path.join(directory, process_date + '.json'), 'w') as json_file:
            json.dump(data_json, json_file)
    except Exception as exc:
        print("Could not write data into file " + os.path.join(directory, process_date + '.json'))


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1
}

dag = DAG(
    'currency_dag',
    description='currency dag',
    schedule_interval='@daily',
    start_date=datetime(2021, 2, 23, 1, 0),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='currency_function',
    dag=dag,
    provide_context=True,
    python_callable=currency
)
