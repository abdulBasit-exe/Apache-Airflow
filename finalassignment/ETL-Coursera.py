from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import pandas as pd 


default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id = 'ETL_toll_data',
    default_args= default_args,
    description= "Apache Airflow Final Assignment",
    schedule_interval= timedelta(days=1),
    catchup=False
)

unzip_data = BashOperator(
    task_id = "unzip",
    bash_command = 'tar -xzvf /home/abdul-basit/airflow/dags/finalassignment/tolldata.tgz -C /home/abdul-basit/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag
)

extracted_data_from_csv = BashOperator(
    task_id = "extracted_from_csv",
    dag = dag,
    bash_command = r"""
    input_csv_path="/home/abdul-basit/airflow/dags/finalassignment/vehicle-data.csv"

    output_csv_path="/home/abdul-basit/airflow/dags/finalassignment/csv_data.csv"

    fields_to_extract = "Rowid, Timestamp, Anonymized Vehicle number, Vehicle_type"
    

    awk -v fields="$fields_to_extract" 'BEGIN {OFS=FS=",";split(fields, extract_fields)} {print $extract_fields}' "$input_csv_path" > "$output_csv_path"

    echo "Extraction complete. Results saved to $output_csv_path"

    """
    )


extracted_data_from_tsv = BashOperator(
    task_id = "extracted_from_tsv",
    dag = dag,
    bash_command = r"""
    input_csv_path="/home/abdul-basit/airflow/dags/finalassignment/tollplaza-data.tsv"

    output_csv_path="/home/abdul-basit/airflow/dags/finalassignment/tsv_data.csv"

    fields_to_extract = "Number of axles, Tollplaza id, Tollplaza code"
    

    awk -v fields="$fields_to_extract" 'BEGIN {OFS=FS=",";split(fields, extract_fields)} {print $extract_fields}' "$input_csv_path" > "$output_csv_path"

    echo "Extraction complete. Results saved to $output_csv_path"

    """
    )

extracted_data_from_fixed_width = BashOperator(
    task_id = "extracted_from_fixed_with",
    dag = dag,
    bash_command = r"""
    input_csv_path="/home/abdul-basit/airflow/dags/finalassignment/payment-data.txt"

    output_csv_path="/home/abdul-basit/airflow/dags/finalassignment/fixed_width_data.csv"

    fields_to_extract = "Type of Payment code, Vehicle Code"
    

    awk -v fields="$fields_to_extract" 'BEGIN {OFS=FS=",";split(fields, extract_fields)} {print $extract_fields}' "$input_csv_path" > "$output_csv_path"

    echo "Extraction complete. Results saved to $output_csv_path"

    """
    )

consolidate_data = BashOperator(
    task_id = 'consolidating',
    dag = dag,
    bash_command = r"""
    cd "$home/abdul-basit/airflow/dags/finalassignment" || exit 
    touch extracted_data.csv
    paste -d ',' *.csv > "$extracted_data.csv"
    """
)


def capitalize_all(input_csv_path= '$home/abdul-basit/airflow/dags/finalassignment/extracted_data',
output_csv_path = '$home/abdul-basit/airflow/dags/finalassignment/transformed_data.csv'):
    
    df = pd.read_csv(input_csv_path)
    df = df.applymap(lambda x: str(x).capitalize())
    df.to_csv(output_csv_path, index=False)

input_csv_path = '$home/abdul-basit/airflow/dags/finalassignment/extracted_data'
output_csv_path = '$home/abdul-basit/airflow/dags/finalassignment/transformed_data.csv'


transformed_data = PythonOperator(
    task_id = 'transforming',
    dag = dag,
    python_callable = capitalize_all
)


unzip_data >> extracted_data_from_csv >> extracted_data_from_tsv >> extracted_data_from_fixed_width >> \
consolidate_data >> transformed_data    