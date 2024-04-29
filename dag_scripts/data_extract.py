from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import json
from datetime import datetime
import s3fs
import csv
from pymongo import MongoClient
import time

# Function to download desired dataset from kaggle using kaggle api 
def download_kaggle_dataset(dataset_name ,download_path,unzip=False) :

    try:
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset_name, download_path, unzip=unzip)
        print (f"dataset download success! find the files in {download_path} folder")
    except Exception as e:
        print (f"error while downloading: {e}")

def mongo_insert(mongo_uri,db_name,collection_name):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    start_time = time.time()

    df = pd.read_csv('./data/tweets.csv')
    data = df.to_dict(orient='records')
    collection.insert_many(data)

    end_time = time.time()
    print (f"The taken to insert all the records using insert_one query is ,{end_time - start_time}")
    client.close()