import re
import pandas as pd
from bs4 import BeautifulSoup
from nltk.tokenize import TweetTokenizer, PunktSentenceTokenizer

tweetTok = TweetTokenizer()
sentenceTok = PunktSentenceTokenizer()
tweets_df = pd.read_csv('data/tweets_raw.csv')

#----------------------

def cleanPipeline(s):
    s = BeautifulSoup(s, 'lxml').get_text()
    s = re.sub(r'@[A-Za-z0-9]+|https?://[A-Za-z0-9./]+|\$[A-Za-z]+', '', s)

    try:
        s = s.decode('utf-8-sig').replace(u'\ufffd', '?')
    except:
        pass

    s = re.sub(r'[^a-zA-Z0-9.,$]', ' ', s)
    
    sentences = [sentence.strip('.') for sentence in sentenceTok.tokenize(s)]
    s = ' '.join(sentences).strip()

    words = tweetTok.tokenize(s)
    return (' '.join(words)).strip()

#----------------------

tweets_df.Text = tweets_df.Text.apply(cleanPipeline)
tweets_df.to_csv('data/tweets_cleaned.csv', index=False)
