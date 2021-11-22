import datetime
import pandas as pd
from tqdm import tqdm

import snscrape.modules.twitter as sntwitter

#----------------------

stocks = {
    'TSLA':['Tesla'],
    'HD':['Home Depot'],
    'BABA': ['Alibaba Group'],
    'NVDA': ['NVIDIA Corporation'],
    'JNJ': ['Johnson & Johnson'],
    'NKE': ['Nike'],
    'TWTR': ['Twitter Inc.'],
    'AAPL': ['Apple'],
    'AMZN': ['Amazon'],
    'XOM': ['Exxon Mobil'],
    'JPM': ['JP Morgan Chase'],
    'AMC': ['AMC Entertainment'],
    'PLUG': ['Plug Power'],
    'PG': ['Procter & Gamble'],
    'PFE': ['Pfizer']
}

#----------------------

n = 50
sdate_str = '2021-01-01'
edate_str = '2021-01-16'
min_likes = 30
min_replies = 3
min_retweets = 3

#----------------------

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
edate_dt = datetime.date.today() if edate_dt.date() >= datetime.date.today() else edate_dt
    
date_range = [c.strftime('%Y-%m-%d') for c in pd.date_range(sdate_dt, edate_dt, freq='d')]
tweets_arr = []

#----------------------

for stock in tqdm(stocks.keys()):

    q_ticker = f'${stock}'

    for d in tqdm(range(len(date_range) - 1)):

        query = f'{q_ticker} since:{date_range[d]} until:{date_range[d+1]} min_faves:{min_likes} min_replies:{min_replies} min_retweets:{min_retweets}'
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
            if i>=n:
                break
            tweets_arr.append(
                [tweet.id, stock, tweet.date,
                tweet.user.displayname, tweet.user.id, tweet.user.followersCount, int(tweet.user.verified), tweet.user.listedCount,
                tweet.likeCount, tweet.retweetCount, tweet.replyCount, 
                tweet.url, tweet.content])

#----------------------

tweets_df = pd.DataFrame(tweets_arr, columns=
                    ['ID', 'Stock', 'Date', 
                    'User_Name', 'User_ID', 'User_Followers', 'User_Verified', 'User_Lists',
                    'Likes', 'Retweets', 'Replies', 
                    'URL', 'Text'])

tweets_df.drop_duplicates(subset=['ID', 'Stock'], ignore_index=True, inplace=True)
tweets_df.to_csv('tweets_raw.csv')
print(f'Scraped {len(tweets_df.index)} Tweets')
