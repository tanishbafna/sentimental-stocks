import snscrape.modules.twitter as sntwitter
import datetime
import pandas as pd
from tqdm import tqdm

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

n = 1
sdate_str = '2021-01-01'
edate_str = '2021-01-02'
min_likes = 10
min_replies = 1
min_retweets = 1

#----------------------

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
edate_dt = datetime.date.today() if edate_dt.date() >= datetime.date.today() else edate_dt
    
date_range = [c.strftime('%Y-%m-%d') for c in pd.date_range(sdate_dt, edate_dt, freq='d')]
tweets_arr = []

#----------------------

for stock in stocks.keys():

    q_ticker = f'${stock}'

    for d in range(len(date_range) - 1):

        query = f'{q_ticker} since:{date_range[d]} until:{date_range[d+1]} min_faves:{min_likes} min_replies:{min_replies} min_retweets:{min_retweets}'
        for i, tweet in tqdm(enumerate(sntwitter.TwitterSearchScraper(query).get_items())):
            if i>=n:
                break
            tweets_arr.append([tweet.id, stock, tweet.date, tweet.user.displayname, tweet.user.id, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.url, tweet.content])

tweets_df = pd.DataFrame(tweets_arr, columns=['ID', 'Stock', 'Date', 'User_Name', 'User_ID', 'Likes', 'Retweets', 'Replies', 'URL', 'Text'])
tweets_df.drop_duplicates(subset=['ID'], ignore_index=True, inplace=True)
tweets_df.set_index('ID', inplace=True)
tweets_df.to_csv('tweets_scraped.csv')
print(f'Scraped {len(tweets_df.index)} Tweets')
