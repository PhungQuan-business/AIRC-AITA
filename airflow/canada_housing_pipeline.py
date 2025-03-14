from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import logging

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'canada_housing_pipeline',
    default_args=default_args,
    description='A DAG to process Canada housing data',
    schedule_interval=None,  # Run manually for now; adjust as needed
    start_date=datetime(2025, 3, 12),
    catchup=False,
) as dag:

    # Task 1: Load Data
    def load_data(**kwargs):
        logging.info("Loading data from CSV files...")
        # Define file paths (adjust paths based on your setup in Docker)
        file_paths = {
            'data_ab': '/opt/airflow/dags/data/data_ab.csv',
            'data_bc': '/opt/airflow/dags/data/data_bc.csv',
            'data_mb': '/opt/airflow/dags/data/data_mb.csv',
            'data_nb': '/opt/airflow/dags/data/data_nb.csv',
            'data_nl': '/opt/airflow/dags/data/data_nl.csv',
            'data_ns': '/opt/airflow/dags/data/data_ns.csv',
            'data_nt': '/opt/airflow/dags/data/data_nt.csv',
            'data_on': '/opt/airflow/dags/data/data_on.csv',
            'data_pe': '/opt/airflow/dags/data/data_pe.csv',
            'data_sk': '/opt/airflow/dags/data/data_sk.csv',
            'data_yt': '/opt/airflow/dags/data/data_yt.csv',
        }

        # Load CSV files into DataFrames
        dataframes = {}
        for key, path in file_paths.items():
            dataframes[key] = pd.read_csv(path, low_memory=False)

        # Concatenate all DataFrames
        df = pd.concat([
            dataframes['data_ab'], dataframes['data_bc'], dataframes['data_mb'],
            dataframes['data_nb'], dataframes['data_nl'], dataframes['data_ns'],
            dataframes['data_nt'], dataframes['data_on'], dataframes['data_pe'],
            dataframes['data_sk'], dataframes['data_yt']
        ], axis=0)

        logging.info(f"Loaded and concatenated data. Shape: {df.shape}")
        # Push the DataFrame to XCom for the next task
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    # Task 2: Initial Cleaning
    def initial_cleaning(**kwargs):
        logging.info("Performing initial cleaning...")
        # Pull the DataFrame from XCom
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='load_data')

        # Drop duplicate rows and columns
        df = df.drop_duplicates()
        df = df.T.drop_duplicates().T

        # Drop rows with missing values in critical columns
        df = df.dropna(subset=["streetAddress", "addressLocality", "addressRegion", "price"])

        logging.info(f"After initial cleaning, shape: {df.shape}")
        # Push the cleaned DataFrame to XCom
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    initial_cleaning_task = PythonOperator(
        task_id='initial_cleaning',
        python_callable=initial_cleaning,
        provide_context=True,
    )

    # Task 3: Transform Square Footage
    def transform_square_footage(**kwargs):
        logging.info("Transforming Square Footage columns...")
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='initial_cleaning')

        # Combine Square Footage and property-sqft
        df.loc[:, 'Square Footage new'] = df['Square Footage'].str.replace(' SQFT', '', regex=False)
        df.loc[:, 'Square Footage new'] = df['Square Footage new'].fillna(df['property-sqft'])
        # Optionally, drop the old columns
        df = df.drop(columns=['Square Footage', 'property-sqft'])
        df = df.rename(columns={'Square Footage new': 'Square Footage'})

        logging.info(f"After Square Footage transformation, shape: {df.shape}")
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    transform_square_footage_task = PythonOperator(
        task_id='transform_square_footage',
        python_callable=transform_square_footage,
        provide_context=True,
    )

    # Task 4: Transform Acreage
    def transform_acreage(**kwargs):
        logging.info("Transforming Acreage column...")
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='transform_square_footage')

        # Fill missing Acreage values with 0
        df["Acreage"] = df["Acreage"].fillna(0)

        logging.info("Acreage transformation completed.")
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    transform_acreage_task = PythonOperator(
        task_id='transform_acreage',
        python_callable=transform_acreage,
        provide_context=True,
    )

    # Task 5: Transform Bathrooms
    def transform_bathrooms(**kwargs):
        logging.info("Transforming Bathrooms columns...")
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='transform_acreage')

        # Combine property-baths and Bath columns
        # Since the notebook prioritizes property-baths, we'll use it and fill missing with Bath
        df['Bathrooms'] = df['property-baths'].fillna(df['Bath'])
        # Drop outliers (e.g., values >= 42 as seen in the notebook)
        df = df[df['Bathrooms'] < 42]
        df = df.drop(columns=['property-baths', 'Bath'])

        logging.info(f"After Bathrooms transformation, shape: {df.shape}")
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    transform_bathrooms_task = PythonOperator(
        task_id='transform_bathrooms',
        python_callable=transform_bathrooms,
        provide_context=True,
    )

    # Task 6: Further Cleaning
    def further_cleaning(**kwargs):
        logging.info("Performing further cleaning...")
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='transform_bathrooms')

        # Rename property-beds to Bedrooms for consistency
        df = df.rename(columns={'property-beds': 'Bedrooms'})

        # Drop rows with missing Bedrooms, Bathrooms, or Square Footage
        df = df.dropna(subset=["Bedrooms", "Bathrooms", "Square Footage"])

        # Filter out rows with Square Footage <= 120
        df = df[df["Square Footage"] > 120]

        # Filter out rows with Price < 50,000
        df = df[df["price"] >= 50_000]

        # Drop the View column
        if 'View' in df.columns:
            df = df.drop(columns=['View'])

        # Rename price to Price for consistency
        df = df.rename(columns={'price': 'Price'})
        # Rename addressRegion to Province and addressLocality to City
        df = df.rename(columns={'addressRegion': 'Province', 'addressLocality': 'City'})
        # Rename latitude and longitude to Latitude and Longitude
        df = df.rename(columns={'latitude': 'Latitude', 'longitude': 'Longitude'})

        logging.info(f"After further cleaning, shape: {df.shape}")
        kwargs['ti'].xcom_push(key='dataframe', value=df)
        return df

    further_cleaning_task = PythonOperator(
        task_id='further_cleaning',
        python_callable=further_cleaning,
        provide_context=True,
    )

    # Task 7: Save Data
    def save_data(**kwargs):
        logging.info("Saving cleaned data to CSV...")
        df = kwargs['ti'].xcom_pull(key='dataframe', task_ids='further_cleaning')

        # Save the DataFrame to a CSV file
        output_path = '/opt/airflow/dags/data/cleaned_canada.csv'
        df.to_csv(output_path, index=False)

        logging.info(f"Data saved to {output_path}")

    save_data_task = PythonOperator(
        task_id='save_data',
        python_callable=save_data,
        provide_context=True,
    )

    # Define task dependencies
    load_data_task >> initial_cleaning_task >> transform_square_footage_task >> transform_acreage_task >> transform_bathrooms_task >> further_cleaning_task >> save_data_task
