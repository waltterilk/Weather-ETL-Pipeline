import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile
import sqlalchemy
from sqlalchemy import create_engine
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Directories for Airflow and Data
dags_folder = "/home/wlk/airflow/dags"
data_folder = "/home/wlk/airflow/dags/data"
database_folder = "/home/wlk/airflow/databases"
database_path = "/home/wlk/airflow/databases/monthly_weather_data.db"


def download_kaggle_dataset():
    """Downloads the Kaggle dataset."""
    api = KaggleApi()
    api.authenticate()
    destination_path = '/home/wlk/airflow/dags'
    api.dataset_download_file('muthuj7/weather-dataset', file_name='weatherHistory.csv', path=destination_path)


def extract_data():
    """Extracts the CSV data."""
    dag_directory = os.path.dirname(__file__)
    
    zip_file_path = os.path.join(dag_directory, 'weatherHistory.csv.zip')
    extract_path = dag_directory
    
    with zipfile.ZipFile(zip_file_path, 'r') as zipref:
        zipref.extractall(extract_path)
    csv_file_path = os.path.join(extract_path, 'weatherHistory.csv')
    df = pd.read_csv(csv_file_path)
    return df


def transform_data(df):
    """Transforms the raw data into a more usable format."""
    # Drop missing values
    weather_data = df.dropna()
    
    # Transform Data
    weather_data['Formatted Date'] = pd.to_datetime(weather_data['Formatted Date'], utc=True)

    # Monthly mode for precipitation type
    monthly_mode = weather_data.groupby(weather_data['Formatted Date'].dt.to_period("M"))['Precip Type'].agg(lambda x: x.mode().iat[0] if not x.mode().empty else None).reset_index()
    monthly_mode.columns = ['Month', 'Mode']
    
    # New feature for Wind Strength
    wind_strength_bins = [0, 1.5, 3.3, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6, float('inf')]
    wind_strength_labels = ['Calm', 'Light Air', 'Light Breeze', 'Gentle Breeze', 'Moderate Breeze', 'Fresh Breeze', 'Strong Breeze', 'Near Gale', 'Gale', 'Strong Gale', 'Storm', 'Violent Storm']
    weather_data['Wind Strength Category'] = pd.cut(weather_data['Wind Speed (km/h)'], bins=wind_strength_bins, labels=wind_strength_labels, right=False)
    
    # Monthly averages for Temperature and Humidity
    monthly_averages = weather_data.groupby(weather_data['Formatted Date'].dt.to_period("M")).agg({
        'Temperature (C)': 'mean',
        'Humidity': 'mean', 
    }).reset_index()
    
    monthly_min = weather_data.groupby(weather_data['Formatted Date'].dt.to_period("M")).agg({
        'Temperature (C)': 'min',
    }).reset_index()
    
    monthly_max = weather_data.groupby(weather_data['Formatted Date'].dt.to_period("M")).agg({
        'Temperature (C)': 'max',
    }).reset_index()
    
    # Construct final monthly dataframe
    df_monthly = pd.DataFrame()
    df_monthly['Date'] = monthly_averages['Formatted Date']
    df_monthly['Average Temperature (C)'] = monthly_averages['Temperature (C)']
    df_monthly['Average Humidity'] = monthly_averages['Humidity']
    df_monthly['Mode'] = monthly_mode['Mode']
    df_monthly['Lowest Temp (C)'] = monthly_min['Temperature (C)']
    df_monthly['Highest Temp (C)'] = monthly_max['Temperature (C)']

    return df_monthly


def load_data_to_db(df, db_uri, table_name):
    """Loads the transformed data to SQLite database."""
    engine = create_engine(db_uri)
    df['Date'] = df['Date'].astype(str)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)


def etl_process():
    """Orchestrates the ETL process: download, transform, load."""
    # Extract Data
    df = extract_data()

    # Transform Data
    transformed_df = transform_data(df)
    
    # Load Data
    load_data_to_db(transformed_df, f'sqlite:///{database_path}', 'monthly_weather_data')
    
    return transformed_df


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 5),
    'retries': 1,
}

dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='A simple weather ETL pipeline',
    schedule_interval='@daily',
)

download_task = PythonOperator(
   task_id='download_task',
   python_callable=download_kaggle_dataset,
   dag=dag,
)

run_etl_process = PythonOperator(
   task_id='run_etl_process',
   python_callable=etl_process,
   dag=dag,
)

download_task >> run_etl_process