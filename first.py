from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',
    default_args= default_args,
    description= "This is the first dag i have created.",
    schedule_interval= timedelta(days=1),
    catchup=False
)

def print_hello():
    print("Hello Airlfow!!")

start_task = PythonOperator(
    task_id = 'print_hello',
    python_callable= print_hello,
    dag= dag,                                                                                                  
)

def end_task():
    print("Endinggggggg")

end_task = PythonOperator(
    task_id = 'End_task',
    dag = dag,
    python_callable = end_task
)

start_task >> end_task

