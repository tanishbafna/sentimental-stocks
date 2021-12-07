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

#----------------------

sdate_str = '2019-01-01'
edate_str = '2021-12-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')

#----------------------

def fetch(stock, period):

    if period == 'y':
        date_range = [f"{c.strftime('%Y-%m-%d')}T00:00:00Z" for c in pd.date_range(sdate_dt, edate_dt, periods=4)]
    elif period == 'q':
        date_range = [f"{c.strftime('%Y-%m-%d')}T00:00:00Z" for c in pd.date_range(sdate_dt, edate_dt, periods=12)]
    elif period == 'm':
        date_range = [f"{c.strftime('%Y-%m-%d')}T00:00:00Z" for c in pd.date_range(sdate_dt, edate_dt, periods=35)]
    
    date_range.pop()
    date_range.append(f"{datetime.date.today()}T00:00:00Z")

    for d in tqdm(range(len(date_range) - 1)):

        done = False
        limit = 1000

        while not done:
            resp = session.get(endpoint, params={'ticker':stock, 'published_utc.gte':date_range[d], 'published_utc.lt':date_range[d+1], 'limit':limit})
            if resp.status_code == 200:
                done = True
                if len(resp.json()['results']) < limit:
                    for r in resp.json()['results']:
                        try:
                            articles_data.append([r['id'], r['published_utc'], r['publisher']['name'], r['title'], r['author'], r['description'], r['article_url']])
                        except:
                            articles_data.append([r['id'], r['published_utc'], r['publisher']['name'], r['title'], r['author'], r['title'], r['article_url']])
                else:
                    return None
            elif resp.status_code == 429:
                time.sleep(60)
            else:
                print(f'{resp.status_code}: {stock} [{date_range[d]}]')

#----------------------

articles_data = []

for stock in tqdm(stocks.keys()):

    sub_data = fetch(stock, 'y') or fetch(stock, 'q') or fetch(stock, 'm')

    if sub_data is None:
        print(stock)
        continue

    articles_data += sub_data

#----------------------

articles_df = pd.DataFrame(columns=['ID', 'Date', 'Publisher', 'Title', 'Author', 'Description', 'URL'], data=articles_data)
articles_df.to_csv('data/temp/financial_news.csv', index=False)
