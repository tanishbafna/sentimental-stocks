import os
import time
import json
import requests
import datetime
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv; load_dotenv()

#----------------------

stocks = json.load(open('stocks.json'))

API_KEY = os.getenv('POLYGON_KEY')
endpoint = 'https://api.polygon.io/v2/reference/news'

session = requests.Session()
session.params['apiKey'] = API_KEY
session.params['order'] = 'asc'
session.params['sort'] = 'published_utc'
session.params['limit'] = 10

#----------------------

sdate_str = '2021-01-01'
edate_str = '2021-01-30'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
edate_dt = datetime.date.today() if edate_dt.date() >= datetime.date.today() else edate_dt

date_range = [f"{c.strftime('%Y-%m-%d')}T00:00:00Z" for c in pd.date_range(sdate_dt, edate_dt, freq='d')]

#----------------------

news_articles = {}

for stock in tqdm(stocks.keys()):
    news_articles[stock] = {}

    for d in tqdm(range(len(date_range) - 1)):

        done = False

        while not done:
            resp = session.get(endpoint, params={'ticker':stock, 'published_utc.gte':date_range[d], 'published_utc.lt':date_range[d+1]})
            if resp.status_code == 200:
                done = True
                if len(resp.json()['results']) > 0:
                    news_articles[stock][date_range[d][:9]] = [[r['id'], r['publisher']['name'], r['article_url'], r['title'], r['author'], r['published_utc'], r['description']] for r in resp.json()['results']]
            elif resp.status_code == 429:
                time.sleep(60)
            else:
                print(f'{resp.status_code}: {stock} [{date_range[d]}]')
    
#----------------------

with open('data/tweets_news.json', 'w') as f:
    json.dump(news_articles, f, indent=2)
