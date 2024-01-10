'''
============================================================================================================
Final Project

Nama  : Tim 2 (Achmad Nauvaldi, Rizqia Dewi Annisa, Sinthiya Kusuma Nagari, Trielianto Stanislaus Kanopi)

Batch : FTDS HCK-010

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. 
Adapun dataset yang dipakai adalah dataset hasil scrapping stockbit Kompas100 dari tahun 2021 - 2023.
============================================================================================================
'''


import pandas as pd
from sqlalchemy import create_engine


# loading DAG
def data_fetch():
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    conn = engine.connect()

    df = pd.read_sql_query('SELECT * FROM "table_FinalProject"', conn)

    # save to .data/ path
    df.to_csv('/opt/airflow/data/data_raw.csv' , sep=',', index=False)