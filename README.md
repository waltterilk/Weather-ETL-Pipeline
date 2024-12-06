# Weather-ETL-Pipeline
This project demonstrates an end-to-end data pipeline for extracting, transforming, and loading (ETL) weather data into a SQLite database. It showcases skills in data engineering, including data extraction via APIs, data cleaning, transformation, and pipeline automation using Apache Airflow.

## Features
- Downloads weather data from Kaggle.
- Cleans and processes data into daily and monthly summaries.
- Loads data into a SQLite database for analysis and visualization.
- Airflow DAG for automated pipeline execution.

## ETL Workflow
1. Extract: Download historical weather data from Kaggle using the Kaggle API.
2. Transform: Clean the dataset, drop missing values, and calculate:
   * Daily averages for temperature and humidity.
   * Monthly summaries with temperature range (min, max) and precipitation mode.
   * Categorization of wind strength into descriptive labels.
3. Load: Save the transformed data into a SQLite database for easy querying.

## Repository Structure
- `dags/`: Contains the Airflow DAG and ETL script.
- `data/`: Placeholder for raw data.
- `databases/`: SQLite database for storing processed weather data.
- `weather_etl_pipeline.ipynb`: Jupyter Notebook with the ETL process explained.
- `README.md`: Project documentation.

## Prerequisites
- Python 3.8+
- Kaggle API (set up authentication as per [Kaggle documentation](https://www.kaggle.com/docs/api))
- Apache Airflow
- SQLite

## Installation
1. Clone the repository
2. Install required packages:
- pip install -r requirements.txt
3. Set up Kaggle API credentials:
- Place your kaggle.json file in ~/.kaggle/.
4. Edit dag file dags_folder, data_folder, database_folder and database_path to correct ones in your own system.
5. Run the ETL process manually:
- python dags/etl_pipeline.py
6. Use Airflow for automation:
- Place the dags/ folder in your Airflow directory.
- Start the Airflow scheduler.

## Usage
- Jupyter Notebook: Open weather_etl_pipeline.ipynb to view the data pipeline process.
- Airflow: Use the DAG to automate data processing.











