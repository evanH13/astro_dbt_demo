from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime
import requests
from io import StringIO
import pandas as pd

# Fetch weather from open-meteo API
@dag(
    dag_id='weather_fetcher',
    schedule='@daily',
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=['weather', 's3', 'snowflake', 'dbt'],
)
def weather_pipeline_dag():
    @task
    def fetch_weather_data_los_angeles():
        # Query the Open-Meteo API for Los Angeles weather data
        response = requests.get("https://api.open-meteo.com/v1/forecast?latitude=35&longitude=139&hourly=temperature_2m")
        # Load the data into a pandas DataFrame
        df = pd.DataFrame(response.json()['hourly'])
        # Convert the DataFrame to CSV format
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the CSV data to S3
        s3 = S3Hook(aws_conn_id="aws_default")
        s3.load_string(
            string_data=csv_buffer.getvalue(),
            key='weather-data/raw_weather.csv',
            bucket_name='evan-weather-bucket',
            replace=True
        )
        return "raw_weather.csv uploaded"
    fetch_weather_data_los_angeles()

dag = weather_pipeline_dag()