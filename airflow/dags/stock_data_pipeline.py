from datetime import datetime, timedelta
import yfinance as yf
import os
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import boto3



AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""  
tikers = 'APPL'
file_name = f"{tikers}_{datetime.now().strftime('%Y%m%d')}.csv"
output_dir = '/opt/airflow/'
os.makedirs(output_dir, exist_ok=True)

file_path = os.path.join(output_dir, file_name)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_stock_data(**kwargs):
    """
    Fetches stock data using yfinance and saves to CSV
    """
    # Configuration
    # Example: Apple stock
    stock_data = {}
    for ticker in tikers:
        stock = yf.Ticker(ticker)
        history = stock.history(period = "1d", interval = "5m")
        stock_data[ticker] = history
    df = pd.concat(stock_data, names = ["Ticker", "Date"] )   
    df.to_csv(file_path)


def upload_to_s3(**kwargs):
    bucket_name = "stock-pipeline"
    s3_client = boto3.client(
            's3',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY, )
    
    object_name = file_name
    s3_client.upload_file(file_path, bucket_name, object_name)


with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    description='Fetch stock data and save to CSV',
    schedule_interval='@daily',  # Runs daily
    catchup=False,
    tags=['stock_data'],
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "Starting stock data fetch at $(date)"',
    )

    fetch_data_task = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
        provide_context=True,
    )

    upload_to_s3_task = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

    end_task = BashOperator(
        task_id='end_task',
        bash_command='echo "Finished stock data fetch at $(date)"',
    )

    # Set task dependencies
    start_task >> fetch_data_task >> upload_to_s3_task >> end_task

