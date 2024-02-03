from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
#from data_extractor.openAPI_extract import extract_data


def extract_data():
    # Load the environment variables from the .env file
    #load_dotenv()

    #city_name = os.environ['city_name']
    #api_key = os.environ['api_key']

    # api endpoint URL

    api_url = f"https://api.openweathermap.org/data/2.5/weather?q=london&appid=797a2c2546bae844a21745b841cc6113" 
    # Send the GET request
    response = requests.get(api_url)

    # Check the response status code
    if response.status_code == 200:
        # Success! Parse the JSON response
        transformed_data_list = []
        weather_data = response.json()
        city = weather_data["name"]
        feels_like= weather_data["main"]["feels_like"]
        min_temp = weather_data["main"]["temp_min"]
        max_temp = weather_data["main"]["temp_max"]
        pressure = weather_data["main"]["pressure"]
        humidity = weather_data["main"]["humidity"]
        wind_speed = weather_data["wind"]["speed"]

        transformed_data = {"City": city,
                            "Feels Like (F)": feels_like,
                            "Minimun Temp (F)":min_temp,
                            "Maximum Temp (F)": max_temp,
                            "Pressure": pressure,
                            "Humidty": humidity,
                            "Wind Speed": wind_speed                        
                            }
        transformed_data_list.append(transformed_data)                  
        df_transformed_data = pd.DataFrame(transformed_data_list)
        
    else:
        # Handle the error
        print("API request failed with status code:", response.status_code)


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
