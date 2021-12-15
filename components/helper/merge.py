import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm

#----------------------

tweets = pd.read_csv('data/sentiment/tweets_sentiment.csv', converters={'Date':lambda x: x[:10]})
news = pd.read_csv('data/sentiment/tweets_sentiment.csv')
prices = pd.read_csv('data/quantitative/stock_prices.csv')

df = pd.read_csv('data/quantitative/stock_prices.csv')
df['tweets_SentMean'] = 0.0
df['tweets_WeightedSentMean'] = 0.0
df['news_SentMean'] = 0.0

#----------------------

for idx, row in tqdm(prices.iterrows(), total=len(prices.index)):

    stock = row['Stock']
    date = row['Date']

    tweets_select = tweets.loc[(tweets.Stock == stock) & (tweets.Date == date)]
    news_select = news.loc[(news.Stock == stock) & (news.Date == date)]
    
    if len(tweets_select.index) > 0:
        tweets_select = tweets_select[['sentiment','weighted_sentiment']]
        sentiment_mean = tweets_select['sentiment'].mean()
        weighted_sentiment_mean = tweets_select['weighted_sentiment'].mean()

        df.at[idx, 'tweets_SentMean'] = sentiment_mean
        df.at[idx, 'tweets_WeightedSentMean'] = weighted_sentiment_mean

    if len(news_select.index) > 0:
        sentiment_mean = news_select['sentiment'].mean()

        df.at[idx, 'news_SentMean'] = sentiment_mean

#----------------------

df.to_csv('data/ml_dataset.csv', index=False)
print(len(df.index))