# import ;ibrary
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from path.data_fetch import data_fetch
from path.data_cleaning import data_cleaning
from path.data_post import data_post

# from datetime import datetime
from sqlalchemy import create_engine, insert
import pandas as pd
import warnings
warnings.filterwarnings('ignore')



default_args = {
    'owner': 'tim2',
    'start_date': dt.datetime(2023, 1, 10),
    'retries': 2,
    'retry_delay': dt.timedelta(minutes=1)
}


with DAG(
    'ETL',
    default_args=default_args,
    schedule_interval='30 6 * * *', # schedule for 6:30 AM WIB
    ) as dag:
    
    data_fetch = PythonOperator(
        task_id='Fetch_from_Postgresql',
        python_callable=data_fetch
    )

    data_cleaning = PythonOperator(
        task_id='Data_Cleaning',
        python_callable=data_cleaning
    )

    data_post = PythonOperator(
        task_id='Post_to_Elasticsearch',
        python_callable=data_post
    )

    # order of execution
    data_fetch >> data_cleaning >> data_post


