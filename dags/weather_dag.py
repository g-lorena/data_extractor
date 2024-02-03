from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from data_extractor.openAPI_extract import extract_data

# Define your Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG('run_data_extractor', default_args=default_args, schedule="40 5 16 * *")

run_extract_apidata = PythonOperator(
    task_id='extract_data_from_API',
    python_callable=extract_data,
    dag=dag
)
run_extract_apidata
