import requests
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import timedelta, datetime


def extract_data():
    # Load the environment variables from the .env file
    load_dotenv()

    city_name = os.environ['city_name']
    api_key = os.environ['api_key']

    # api endpoint URL

    api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}" 
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
        return df_transformed_data
    else:
        # Handle the error
        print("API request failed with status code:", response.status_code)


def load_csv(df_data):
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'london_weather_' + dt_string
    df_data.to_csv(f"s3://weather-api-datalake/{dt_string}.csv", index=False)


