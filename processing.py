import json
from datetime import datetime
import s3fs
from config import *
import csv
from pymongo import MongoClient
import time
import pandas as pd
from textblob import TextBlob
from utility import day_mapping

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
df['date_time'] = pd.to_datetime(df['date_time'], format='%d/%m/%Y %H:%M')   #converting to standart date time format so that we can use .dt accessor of pandas
df['activity_hour']= df['date_time'].dt.hour
df['activity_day']= df['date_time'].dt.dayofweek
df['activity_month']= df['date_time'].dt.month
df['activity_year'] = df ['date_time'].dt.year
df['engagement'] = df['number_of_likes'] + df ['number_of_shares']
df['activity_day'] = df['activity_day'].map(day_mapping())

# print(df['activity_day'].dtype)
# print(df['activity_day'].unique())
# print(df.head())

df.to_csv('s3://abhi-data/data/tweets_processed.csv')

print("data successfully uploaded to the s3 bucket")