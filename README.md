# Weather-ETL-Pipeline
This project demonstrates an end-to-end data pipeline for extracting, transforming, and loading (ETL) weather data into a SQLite database. It showcases skills in data engineering, including data extraction via APIs, data cleaning, transformation, and pipeline automation using Apache Airflow.

## Features
* Downloads historical weather data from Kaggle.
* Cleans and transforms data into daily and monthly summaries.
* Automates the pipeline with Apache Airflow.
* Loads transformed data into a SQLite database for further analysis.
* Categorizes wind speed into descriptive strength categories (e.g., Gentle Breeze, Storm).

## ETL Workflow
1. Extract: Download historical weather data from Kaggle using the Kaggle API.
2. Transform: Clean the dataset, drop missing values, and calculate:
   * Daily averages for temperature and humidity.
   * Monthly summaries with temperature range (min, max) and precipitation mode.
   * Categorization of wind strength into descriptive labels.
3. Load: Save the transformed data into a SQLite database for easy querying.


