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
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


# exporting to elasticsearch DAG
def data_post():
    es = Elasticsearch(hosts=["http://elasticsearch:9200"])

    df = pd.read_csv('/opt/airflow/data/FP_tim-2_data_clean.csv')
    actions = [
        {
            "_index": "table_finalproject",
            "_source": row.to_dict()
        }
        for i, row in df.iterrows()
    ]
    bulk(es, actions)