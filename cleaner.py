import re
import pandas as pd
from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer

tok = WordPunctTokenizer()
tweets_df = pd.read_csv('tweets.csv')

def cleanPipeline(s):
    s = BeautifulSoup(s, 'lxml').get_text()
    s = re.sub(r'@[A-Za-z0-9]+|https?://[A-Za-z0-9./]+', '', s)

    try:
        s = s.decode('utf-8-sig').replace(u'\ufffd', '?')
    except:
        pass

    s = re.sub(r'[^a-zA-Z]', ' ', s)
    
    words = tok.tokenize(s)
    return (' '.join(words)).strip()

tweets_df.Text = tweets_df.Text.apply(cleanPipeline)
print(tweets_df.Text)
tweets_df.to_csv('tweets_cleaned.csv', index=False)