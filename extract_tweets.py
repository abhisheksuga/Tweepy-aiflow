import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs
from config import *
import csv
from pymongo import MongoClient

# access_key = access_key
# access_key_secret = access_key_secret
# api_token = api_token
# api_token_secret = api_token_secret

# #authetication 

# auth = tweepy.OAuthHandler(access_key,access_key_secret)
# auth.set_access_token(api_token,api_token_secret)

# #API object
# api = tweepy.API(auth)


# tweets = api.user_timeline(screen_name = '@elonmusk',
#                                         count = 200,
#                                         include_rts = False,
#                                         tweet_mode = 'extented')



# print (tweets)


# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

with open('tweets.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        collection.insert_one(row)

client.close()



#https://abhi-data.s3.eu-west-1.amazonaws.com/tweets.csv 