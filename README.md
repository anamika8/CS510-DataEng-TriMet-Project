# TriMet Data Engineering Project

## Overview
This project was completed as part of the Data Engineering course in collaboration with **Vaughn Paulo**. The goal was to consume, process, and analyze TriMet bus "breadcrumb data" provided via a public API. This data includes bus location updates every 60 seconds, which are then combined with bus route information scraped from the TriMet website. We built a pipeline to ingest, clean, validate, and transform this data before loading it into a database for further analysis and visualization.

## Project Features
- **Data Ingestion**: A Python-based script to fetch real-time bus location data via API calls.
- **Data Transformation**: Applied various transformations to ensure data quality and usability.
- **Pipeline Automation**: We set up a daily data ingestion pipeline to automate the process using Apache Kafka for data streaming and email alerts for status notifications.
- **Database Integration**: Cleaned and transformed data was loaded into a database for storage and analysis.
- **Visualizations**: We created visualizations to understand bus activity patterns, using GeoJSON and TSV files for mapping the data.

## File Descriptions

- **`producer.py`**: Script to make API calls for fetching real-time bus data and initiate data transformations.
- **`assertions.py`**: Performs data quality checks to ensure integrity. Raises errors if checks fail.
- **`transformation.py`**: Contains functions to clean and transform the data. This includes calculating speed for each bus trip, removing rows with negative values for speed, and reordering the columns for better analysis. Unneeded columns are dropped.
- **`consumer.py`**: Consumes real-time bus data using Apache Kafka.
- **`load_into_db.py`**: Loads processed data into the database for later analysis.
- **`trip_data_collection.py`**: Scrapes bus route data from the TriMet website, such as trip details, vehicle IDs, and routes.
- **`trip_assertions.py`**: Performs data quality checks on the bus route data, ensuring fields like `trip_id` are valid and consistent. Logs validation results to a file.
- **`trip_transformation.py`**: Cleans and transforms the scraped bus route data, mapping values for `service_key` and `direction` to user-friendly formats, and removes invalid rows with missing or negative values.
- **`trip_producer.py`**: Produces bus route data for consumption using Apache Kafka.
- **`trip_consumer.py`**: Consumes bus route data using Kafka and checks the integrity of the messages received.
- **`trip_load_into_db.py`**: Loads cleaned and validated bus route data into the database for analysis.
- **`table_creation.sql`**: SQL file to create necessary database tables (run once).
- **`email-alert.py`**: A Python script that sends email notifications about the status of the daily data load, whether successful or failed.
- **`visualizations/`**: Contains code to launch a website showcasing the seven visualizations created for analyzing bus activity. It also contains GeoJSON and TSV files for visualization.

## How to Run

1. **Set up the Database**: Run the `table_creation.sql` file to create the required database tables.
2. **Configure the API**: Ensure the API credentials and endpoints are properly set in `producer.py`.
3. **Data Pipeline**:
   - Use `producer.py` and `trip_producer.py` to fetch data from the TriMet API and the website.
   - Data quality checks will be performed via `assertions.py` and `trip_assertions.py`.
   - Transformation logic is applied using `transformation.py` and `trip_transformation.py`.
   - Kafka will manage the streaming process using `consumer.py` and `trip_consumer.py`.
4. **Data Loading**: Load the processed data into the database with `load_into_db.py` and `trip_load_into_db.py`.
5. **Email Notifications**: Run `email-alert.py` to get daily email updates on the data load status.
6. **Visualizations**: Launch the visualizations via the code in the `visualizations` folder. Detailed instructions are provided in the `readme` file within the folder.

## Visualizations
We created seven visualizations to analyze TriMet bus activity patterns over time. These include visual representations of bus routes, locations, and operational efficiency. Visualizations were built using GeoJSON and TSV data for accurate geographical mapping.
