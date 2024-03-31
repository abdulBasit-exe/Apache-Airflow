from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'abdul-basit',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_with_cron_expression_v03',
    default_args= default_args,
    description= "this is a dag with cron expression.",
    start_date = datetime(2024, 3, 20),
    schedule_interval= '17 2 20 3 wed'
) as dag: 
    task1 = BashOperator(
        task_id = 'task1',
        bash_command = "echo 'dag with cron expression!'" 
 )
    task2 = BashOperator(
        task_id = "task2",
        bash_command = "echo '\"done with cron.\"'"
    )


task1 >> task2