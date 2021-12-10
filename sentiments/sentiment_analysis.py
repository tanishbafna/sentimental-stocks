import nltk
from tqdm import tqdm
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#----------------------

tqdm.pandas()
nltk.download('vader_lexicon')
sentiment = SentimentIntensityAnalyzer()

#----------------------

def weightedSentiment(row):

    score = row['sentiment']
    score *= row['Retweets'] * row['Replies'] * row['Likes']

    user_verified = {0:1, 1:1.25}
    user_followers = {0:1, 500:1.1, 1000:1.15, 5000:1.2, 10000:1.3, 50000:1.4, 100000:1.5, 200000:1.6, 300000:1.7, 500000:1.8, 1000000:1.9, max_followers:2}
    user_lists = {0:1, 100:1.1, 1000:1.2, 5000:1.3, 10000:1.4, max_lists:1.5}

    user_followers_keys = list(user_followers.keys())
    user_lists_keys = list(user_lists.keys())

    score *= user_verified[row['User_Verified']]
    score *= user_followers[user_followers_keys[min(range(len(user_followers_keys)), key = lambda i: abs(user_followers_keys[i]-row['User_Followers']))]]
    score *= user_lists[user_lists_keys[min(range(len(user_lists_keys)), key = lambda i: abs(user_lists_keys[i]-row['User_Lists']))]]

    return score

def avgSentiment(row):

    compound = (row['compound_t'] + row['compound_d']) / 2
    return compound

def twitterSentiment(df) -> pd.DataFrame:

    df['sentiment'] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Cleaned'])['compound'], axis=1)
    df['weighted_sentiment'] = df.progress_apply(weightedSentiment, axis=1) 
    return df

def newsSentiment(df) -> pd.DataFrame:

    df['compound_t'] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Title_Cleaned'])['compound'], axis=1)
    df['compound_d'] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Description_Cleaned'])['compound'], axis=1)
    
    df['sentiment'] = df.progress_apply(avgSentiment, axis=1)
    df.drop(columns=['compound_t','compound_d'], inplace=True)
    return df

#----------------------

file = 'news_cleaned'
path = f'data/qualitative/{file}.csv'

data = pd.read_csv(path)
# max_followers, max_lists = data.User_Followers.max(), data.User_Lists.max()

newsSentiment(data).to_csv(f'data/sentiment/{file.replace("_cleaned","")}_sentiment.csv', index=False)
