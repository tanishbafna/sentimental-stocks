import snscrape.modules.twitter as sntwitter
import datetime
import pandas as pd
from tqdm import tqdm

#----------------------

stocks = ['TSLA']

#----------------------

n = 1000
sdate_str = '2020-01-01'
edate_str = '2020-01-31'
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

for stock in stocks:

    q_ticker = f'${stock}'

    for d in range(len(date_range) - 1):

        query = f'{q_ticker} since:{date_range[d]} until:{date_range[d+1]} min_faves:{min_likes} min_replies:{min_replies} min_retweets:{min_retweets}'
        for i, tweet in tqdm(enumerate(sntwitter.TwitterSearchScraper(query).get_items())):
            if i>n:
                break
            tweets_arr.append([tweet.id, tweet.date, tweet.user.username, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.url, tweet.content])
        
        # q_hashtag = stock
        # query = f'{q_ticker} since:{date_range[d]} until:{date_range[d+1]} min_faves:{min_likes} min_replies:{min_replies} min_retweets:{min_retweets}'
        # for i, tweet in tqdm(enumerate(sntwitter.TwitterHashtagScraper(query).get_items())):
        #     if i>n:
        #         break
        #     tweets_arr.append([tweet.id, tweet.date, tweet.user.username, tweet.likeCount, tweet.retweetCount, tweet.replyCount, tweet.url, tweet.content])

tweets_df = pd.DataFrame(tweets_arr, columns=['ID', 'Date', 'Username', 'Likes', 'Retweets', 'Replies', 'URL', 'Text'])
tweets_df.drop_duplicates(subset=['ID'], ignore_index=True, inplace=True)
tweets_df.set_index('ID', inplace=True)
tweets_df.to_csv('tweets.csv')
print(f'Scraped {len(tweets_df.index)} Tweets')