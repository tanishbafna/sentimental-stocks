import nltk
from tqdm import tqdm
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer

#----------------------

tqdm.pandas()
nltk.download('vader_lexicon')
sentiment = SentimentIntensityAnalyzer()

#----------------------

def avgSentiment(row):

    neg = (row['neg_t'] + row['neg_d']) / 2
    neu = (row['neu_t'] + row['neu_d']) / 2
    pos = (row['pos_t'] + row['pos_d']) / 2
    compound = (row['compound_t'] + row['compound_d']) / 2

    return [neg, neu, pos, compound]

def twitterSentiment(df) -> pd.DataFrame:

    df[['neg', 'neu', 'pos', 'compound']] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Cleaned']).values(), result_type='expand', axis=1)
    return df

def newsSentiment(df) -> pd.DataFrame:

    df[['neg_t', 'neu_t', 'pos_t', 'compound_t']] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Title_Cleaned']).values(), result_type='expand', axis=1)
    df[['neg_d', 'neu_d', 'pos_d', 'compound_d']] = df.progress_apply(lambda row:sentiment.polarity_scores(row['Description_Cleaned']).values(), result_type='expand', axis=1)
    
    df[['neg', 'neu', 'pos', 'compound']] = df.progress_apply(avgSentiment, result_type='expand', axis=1)
    df.drop(columns=['neg_t', 'neu_t', 'pos_t', 'compound_t','neg_d', 'neu_d', 'pos_d', 'compound_d'], inplace=True)

    return df

#----------------------

file = 'news_cleaned'
path = f'data/qualitative/{file}.csv'

data = pd.read_csv(path)
newsSentiment(data).to_csv(f'data/sentiment/{file}_sentiment.csv', index=False)
