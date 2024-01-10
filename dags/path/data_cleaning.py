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



def data_cleaning():
     # load
    '''  Fungsi ini ditujukan untuk membersihkan data.
    output: FP_tim-2_data_clean.csv (data yang telah bersih)
    '''
    data = pd.read_csv("/opt/airflow/data/data_raw.csv") # read csv

    # filter data

    data.dropna(inplace=True) # delete missing value

    data.drop_duplicates(inplace=True) # delete duplicate data

    data.drop('Unnamed: 0', axis=1, inplace=True)

    data.drop('index', axis=1, inplace=True)

    for i in data.columns:
        i_lower = i.lower() # change column name to lower case
        i_cleaned = i_lower.replace(' ', '_').replace('-', '_') # change space to underscore
        data.rename(columns={i: i_cleaned}, inplace=True)
        data.columns = [col.translate(i_cleaned) for col in data.columns] 
    data.to_csv('/opt/airflow/data/FP_tim-2_data_clean.csv', index=True) # saving the cleaned data

