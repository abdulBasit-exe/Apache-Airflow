from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from datetime import datetime, timedelta
import pendulum
import requests
import xmltodict
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner' : 'abdul-basit',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="podcast_summary",
    default_args = default_args,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2024, 3,26),
    catchup=False
)

def podcast_summary():
    create_database = PostgresOperator(
        task_id='create_table_postgres',
        sql=r"""
        CREATE TABLE IF NOT EXISTS episodes (
            link TEXT PRIMARY KEY,
            title TEXT,
            filename TEXT,
            published TEXT,
            description TEXT,
            transcript TEXT
        );
        """,
        postgres_conn_id= 'postgres_default'
        # hook_params={"database":'airflow_db','schema': 'airflow'},
    )

    @task()
    def get_episodes():
        data = requests.get("https://www.marketplace.org/feed/podcast/marketplace/")
        feed = xmltodict.parse(data.text)
        episodes = feed['rss']["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = get_episodes()
    create_database.set_downstream(podcast_episodes)


    @task()
    def load_episodes(episodes):
        hook = PostgresHook(conn_id = 'podcast')
        stored = hook.get_pandas_df("SELECT * FROM public.\"episodes\";")
        new_episodes = []
        for episode in episodes: 
            if episode['link'] not in stored["link"].values:
                filename = f"{episode['link'].split('/')[-1]}.mp3"
                new_episodes.append([episode['link'], episode["title"],episode["pubDate"], episode["description"], filename])
        hook.insert_rows(table="episodes", rows=new_episodes, target_fields=["link", "title", "published", "description", "filename"])

    load_episodes(podcast_episodes)


summary = podcast_summary()
