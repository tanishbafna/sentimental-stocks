import os
import json
import datetime
import pandas as pd
from tqdm import tqdm

import snscrape.modules.twitter as sntwitter

#----------------------

os.mkdir('data/progress')
stocks = json.load(open('stocks.json'))

n = 5
sdate_str = '2020-01-01'
edate_str = '2020-01-15'
min_likes = 30
min_replies = 1
min_retweets = 10

#----------------------

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
edate_dt = datetime.date.today() if edate_dt.date() >= datetime.date.today() else edate_dt
date_range = [c.strftime('%Y-%m-%d') for c in pd.date_range(sdate_dt, edate_dt, freq='d')]

tweets_arr = []
errors_arr = {stock:{} for stock in stocks.keys()}

#----------------------

for stock in tqdm(stocks.keys()):

    sub_array = []
    q_ticker = f'${stock}'

    #-------

    for d in tqdm(range(len(date_range) - 1)):

        try:
            query = f'{q_ticker} since:{date_range[d]} until:{date_range[d+1]} min_faves:{min_likes} min_replies:{min_replies} min_retweets:{min_retweets}'
            for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
                if i>=n:
                    break
                sub_array.append(
                    [tweet.id, stock, tweet.date,
                    tweet.user.displayname, tweet.user.id, tweet.user.followersCount, int(tweet.user.verified), tweet.user.listedCount,
                    tweet.likeCount, tweet.retweetCount, tweet.replyCount, 
                    tweet.url, tweet.content])
        except Exception as e:
            errors_arr[stock][date_range[d]] = e
    
    #-------

    sub_df = pd.DataFrame(sub_array, columns=
                    ['ID', 'Stock', 'Date', 
                    'User_Name', 'User_ID', 'User_Followers', 'User_Verified', 'User_Lists',
                    'Likes', 'Retweets', 'Replies', 
                    'URL', 'Text'])

    #-------

    sub_df.drop_duplicates(subset=['ID', 'Stock'], ignore_index=True, inplace=True)
    sub_df.to_csv(f'data/progress/{stock}.csv')

    tweets_arr += sub_array

#----------------------

tweets_df = pd.DataFrame(tweets_arr, columns=
                    ['ID', 'Stock', 'Date', 
                    'User_Name', 'User_ID', 'User_Followers', 'User_Verified', 'User_Lists',
                    'Likes', 'Retweets', 'Replies', 
                    'URL', 'Text'])

tweets_df.drop_duplicates(subset=['ID', 'Stock'], ignore_index=True, inplace=True)
tweets_df.to_csv('data/tweets_raw.csv')
print(f'Scraped {len(tweets_df.index)} Tweets')

#----------------------

with open('data/errors.json', 'w') as f:
    json.dump(errors_arr, f, indent=2)

#----------------------
