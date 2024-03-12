from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta



default_args = {
    'owner' : 'abdul-basit',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
}

@dag(dag_id='dag_with_taskflow',
     default_args = default_args,
     start_date= datetime(2024,3,7),
     description = "this dag is created through taskflow Api",
     schedule_interval = timedelta(hours=1))

def greeting_hello_world():

    @task
    def getName():
        return "abdul basit"

    @task
    def getAge():
        return 22

    @task
    def greet(name, age):
        print(f"Hello World, this is {name} "
              f"and i am {age} years old.")
    
    name = getName()
    age = getAge()
    greet(name = name, age = age)

greet_dag = greeting_hello_world()