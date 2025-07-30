from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Add scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

from transform_data import extract_and_transform

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 29),
    'retries': 1,
}

with DAG(
    'transactional_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_transform_task = PythonOperator(
        task_id='extract_and_transform',
        python_callable=extract_and_transform,
        op_kwargs={
            'csv_path': '/opt/airflow/data/raw/transactions.csv',
            'json_path': '/opt/airflow/data/raw/customers.json',
            'api_url': 'https://jsonplaceholder.typicode.com/posts',
            'output_path': '/opt/airflow/data/processed/transformed_data.csv'
        }
    )

    # Placeholder for dbt task (added in Step 5)
    extract_transform_task
