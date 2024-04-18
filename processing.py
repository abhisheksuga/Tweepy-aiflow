import json
from datetime import datetime
import s3fs
from config import *
import csv
from pymongo import MongoClient
import time
import pandas as pd
from textblob import TextBlob


def count_of_words(content):
    return len(content.split())

def extract_sentiment(content) :
    emotions = TextBlob(content)
    return emotions.sentiment.polarity

def sentiment_category (polarity) :
    if polarity > 0 :
        return 'positive'
    if polarity < 0 :
        return 'negative'
    else:
        return 'neutral'



client = MongoClient(mongo_uri)

db = client[db_name]

collection = db[collection_name]


data = collection.find({},{'_id' :0})
data_dict = []
for i in data:
    data_dict.append(i)


df = pd.DataFrame(data=data_dict)
df = df.drop(columns = ['id','latitude','longitude','country'])
df['num_words'] = df['content'].apply(count_of_words)
df['sentiment_polarity'] = df['content'].apply(extract_sentiment)
df['sentiment_category'] = df['sentiment_polarity'].apply(sentiment_category)


print(df.head())