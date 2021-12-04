import re
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from nltk.tokenize import TweetTokenizer, PunktSentenceTokenizer, WordPunctTokenizer

tweetTok = TweetTokenizer()
wordTok = WordPunctTokenizer()
sentenceTok = PunktSentenceTokenizer()

#----------------------

def cleanPipelineTwitter(s):
    
    try:
        s = BeautifulSoup(s, 'lxml').get_text()
    except:
        print(s)
        pass

    s = re.sub(r'@[A-Za-z0-9]+|https?://[A-Za-z0-9./]+|\$[A-Za-z]+', '', s)

    try:
        s = s.decode('utf-8-sig').replace(u'\ufffd', '?')
    except:
        pass

    # s = re.sub(r'[^a-zA-Z0-9.,$]', ' ', s)
    
    sentences = [sentence.strip('.') for sentence in sentenceTok.tokenize(s)]
    s = ' '.join(sentences).strip()

    words = tweetTok.tokenize(s)
    return (' '.join(words)).strip()

#----------------------

def cleanPipelineNews(s):

    try:
        s = s.decode('utf-8-sig').replace(u'\ufffd', '?')
    except:
        pass
    
    words = wordTok.tokenize(s)

    return (' '.join(words)).strip()

#----------------------

# tweets_df = pd.read_csv('data/tweets_raw.csv')
# tweets_df['Cleaned'] = np.NaN
# tweets_df.Cleaned = tweets_df.Text.apply(cleanPipelineTwitter)
# tweets_df.to_csv('data/tweets_cleaned.csv', index=False)

#----------------------

news_df = pd.read_csv('data/news_raw.csv')
news_df['Description'] = news_df.apply(lambda row: row['Title'] if row['Description'] is np.NaN else row['Description'], axis=1)

news_df['Title_Cleaned'] = news_df.apply(lambda row: cleanPipelineNews(row['Title']), axis=1)
news_df['Description_Cleaned'] = news_df.apply(lambda row: cleanPipelineNews(row['Description']), axis=1)

news_df.to_csv('data/news_cleaned.csv', index=False)
