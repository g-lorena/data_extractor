from openAPI_extract import extract_data
from openAPI_extract import load_csv
from airflow.decorators import dag, task
import pendulum


@dag(
    schedule_interval="0 10 * * *", 
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather_api"],
)

def taskflow_el():
    @task
    def extract():
        df_data = extract_data()
        return df_data
    @task
    def load(df_data: object):
        load_csv(df_data)

    extract_data_from_api = extract()
    load_data_into_csv = load(extract_data_from_api)

el_dag = taskflow_el()