# import the module
import os
from numpy import result_type
import tweepy
import pandas as pd
from dotenv import load_dotenv; load_dotenv()

#---------------------

API_KEY = os.getenv('API_KEY')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')

auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

#----------------------

tweets = pd.read_csv('tweets_scraped.csv')
user_ids = {u:None for u in tweets.User_ID.unique()}

for user_id in user_ids.keys():
    user = api.get_user(user_id=user_id)
    user_ids[user_id] = [user.followers_count, int(user.verified), user.listed_count]

#----------------------

tweets[['User_Followers', 'User_Verified', 'User_Lists']] = tweets.apply(lambda row: user_ids[row.User_ID], axis=1, result_type='expand')
tweets.to_csv('tweets_userInfo.csv', index=False)