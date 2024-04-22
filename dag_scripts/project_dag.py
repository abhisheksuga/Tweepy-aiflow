from datetime import timedelta
from airflow import DAG  
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from project_etl import download_kaggle_dataset



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

# Task to run the mongo_insert function

run_download =PythonOperator (
    task_id = 'data_download',
    python_callable = download_kaggle_dataset,
     op_kwargs={
        'dataset_name': 'mmmarchetti/tweets-dataset',
        'download_path': './data',
        'unzip': True
    },
    dag = dag ,
)
# Task to run the mongo_insert function
run_insert = PythonOperator(
    task_id='insert_to_mongo',
    python_callable=mongo_insert,
    op_kwargs={
        'mongo_uri': 'mongodb+srv://Admin:hGNqRUPelBqwazKk@mymongo.qxtqdes.mongodb.net/',
        'db_name': 'tweets',
        'collection_name': 'test'
    },
    dag=dag,
)


run_download >> run_insert