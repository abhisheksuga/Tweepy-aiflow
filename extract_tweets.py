import pandas as pd
import tweepy
import json
from datetime import datetime
import s3fs
from config import *


access_key = access_key
access_key_secret = access_key_secret
api_token = api_token
api_token_secret = api_token_secret

#authetication 

auth = tweepy.OAuthHandler(access_key,access_key_secret)
auth.set_access_token(api_token,api_token_secret)

#API object
api = tweepy.API(auth)


tweets = api.user_timeline(screen_name = '@elonmusk',
                                        count = 200,
                                        include_rts = False,
                                        tweet_mode = 'extented')



print (tweets)
