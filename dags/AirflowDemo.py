import requests
import json
import os
import time
import boto3
import airflow.hooks.S3_hook

from airflow import DAG
from airflow.models import Variable
from airflow.operators import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook


## APi key to connect to weather api
API_KEY=Variable.get("weather_api_key")

# Following are defaults which can be overridden later on
# dag variables
default_args = {
    'owner': 'Srilekha',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 6),
    'email': ['srilekha.balasubramani@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


## Added for bash operator demo
def my_sleeping_function(random_base):
    """This is a function that will run within the DAG execution"""
    time.sleep(random_base)

# Function which calls get weather api and downloads data to data directory
def get_weather(**kwargs):
    parameters = {'q': 'California,USA', 'appid': API_KEY}
    result = requests.get("https://api.openweathermap.org/data/2.5/weather?", parameters)
    if result.status_code == 200:
        json_data = result.json()
        file_name = str(datetime.now().date()) + '.json'
        tot_name = os.path.join(os.path.dirname(__file__),"data",file_name)

        with open(tot_name, 'w') as outputfile:
            json.dump(json_data, outputfile)
    else:
        print("Error in API call")

## Connects to aws and writes json file to s3
def Write_To_S3(**kwargs):
    ## read aws credentials from configuration
    aws_access_key_id_var = Variable.get("aws_access_key_id")
    aws_secret_access_key_var = Variable.get("aws_secret_access_key")
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id_var,
                        aws_secret_access_key=aws_secret_access_key_var)
    ## printing buckets to check for access
    for bucket in s3.buckets.all():
        print(bucket.name)
    file_name = str(datetime.now().date()) + '.json'
    s3.Bucket("airflow-demo-bucket").upload_file("/usr/local/airflow/dags/data/" + file_name, "out_file.json")
    ## Alternatively use s3 hooks to do this operation
    #hook = airflow.hooks.S3_hook.S3Hook('my_S3_conn')
    #hook.load_file("/usr/local/airflow/dags/data/2020-11-08.json", "out_file.json", "airflow-demo-bucket")


def ConnectToDB(**kwargs):
    curr_dt = str(datetime.now().date())
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    dts_insert = 'insert into public.load_status (job_name, description) values (%s, %s)'
    pg_hook.run(dts_insert, parameters=("Temp_load", curr_dt))

dag = DAG('AirflowDemo', default_args=default_args)

## Example of Bash Operator
t1 = BashOperator(
    task_id='dummy_task_1',
    bash_command='echo "Hello World from Task 1"',
    dag=dag)

t2 = BashOperator(
    task_id='demo_print_date',
    bash_command='date',
    dag=dag)

t1 >> t2

## Example of Python Operator
i = 5
task = PythonOperator(
        task_id='snoozing_for_' + str(i),
        python_callable=my_sleeping_function,
        op_kwargs={'random_base': float(i) / 10},
        dag=dag,
)

t2 >> task

## connect to api end points and create flat file from api data
task2 = PythonOperator(
        task_id='get_weather_api_call',
        python_callable=get_weather,
        dag=dag,
)

## Connecting using aws hook - create and access s3 buckets or write files to s3
task3 = PythonOperator(
        task_id='Write_To_S3',
        python_callable=Write_To_S3,
        dag=dag,
)

## Connect to postgres or rds - using posgres hook - insert or update
task4 = PythonOperator(
        task_id='ConnectToDB',
        python_callable=ConnectToDB,
        dag=dag,
)

task5 = DummyOperator(task_id='pipeline_completed', dag=dag)

task2 >> task3 >> task4

[task, task4] >> task5
