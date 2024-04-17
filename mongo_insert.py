import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs
from config import *
import csv
from pymongo import MongoClient
import time


# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]


start_time = time.time()

# df = pd.read_csv('tweets.csv')
# data = df.to_dict(orient='records')
# collection.insert_many(data)

with open('tweets.csv','r') as csvfile :
    reader = csv.DictReader(csvfile)
    data = list(reader)
    collection.insert_many(data)


end_time = time.time()

print (f"The taken to insert all the records using inssert_one query is ,{end_time - start_time}")

client.close()




# Total time taken for insertion using df.to_dict is ~ 20 seconds 