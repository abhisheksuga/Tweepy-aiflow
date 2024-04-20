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
from db import *
from sqlalchemy import create_engine

#establishing a mongo connection 
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]



connection = connect_postgres(postgres_user,postgres_password,postgres_host,postgres_port,postgres_database) 



#function to get the total count of words in the tweet
def count_of_words(content):
    return len(content.split())

#function to extract the sentiment polarity of the tweet
def extract_sentiment(content) :
    emotions = TextBlob(content)
    return emotions.sentiment.polarity

#function to categorise the sentiment polarities 
def sentiment_category (polarity) :
    if polarity > 0 :
        return 'positive'
    if polarity < 0 :
        return 'negative'
    else:
        return 'neutral'


data = collection.find({},{'_id' :0}) #fetching the records from mongo excluding the mongo objectid 
data_dict = []
for i in data:
    data_dict.append(i)


df = pd.DataFrame(data=data_dict)
print(df)
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
print(df.head())
print (df.dtypes)
df.to_csv('./data/processed_data.csv')
def create_table_from_dataframe(connection,df, table_name):
    try:
        print("sissdabcbavjkbsjkdv ksj v")

        cursor = connection.cursor()

        columns = ', '.join([f"{col_name} {PG_DATA_TYPES[str(data_type)]}" for col_name, data_type in zip(df.columns, df.dtypes)])
        sql_create_table = f"CREATE TABLE {table_name} ({columns})"
        cursor.execute(sql_create_table)
        connection.commit()

        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        print("sissdabcbavjkbsjkdv ksj v")
    finally:
        cursor.close()