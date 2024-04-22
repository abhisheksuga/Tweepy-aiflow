from datetime import timedelta 
from airflow import DAG  
# from airflow.operators.python_operators import PythonOperator 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from dataset_download_kaggle_api import download_kaggle_dataset



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,4,1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'project_dag',
    default_args=default_args,
    description='DAP PROJECT',
    schedule_interval=timedelta(days=1),
)


run_etl =PythonOperator (
    task_id = 'data_etl',
    python_callable = download_kaggle_dataset,  #function that i want to call 
    dag = dag ,
)



run_etl