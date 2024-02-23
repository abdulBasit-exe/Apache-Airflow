# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
from faker import Faker 
import csv

import datetime as dt 
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


output = open('file.csv', mode='w')
fake=Faker()
header=['name','age','street','city','state','zip','lng','lat']
mywriter=csv.writer(output)
mywriter.writerow(header)
for r in range(1000):
    mywriter.writerow([fake.name(),fake.random_int(min=18,
    max=80, step=1), fake.street_address(), fake.city(),fake.
    state(),fake.zipcode(),fake.longitude(),fake.latitude()])
output.close()


def CSVToJson():
    df = pd.read_csv('/home/abdul-basit/airflow/dags/file.csv')
    for i, r in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.json', orient='records')




default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime(2024, 2, 24, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'CSV-To-JSON',
    default_args= default_args,
    description= "CSV to JSON DataPipeline",
    schedule_interval= timedelta(hours=1),
    catchup=False
)

print_starting = BashOperator(
    task_id='starting', 
    bash_command='echo "I am reading the CSV now....."',
    dag= dag,
    
)

CSVJson = PythonOperator(
    task_id= 'convertCSVtoJSON',
    python_callable= CSVToJson,
    dag= dag,
)


print_starting >> CSVJson