import os
import json
import finnhub
import datetime
import pandas as pd
from tqdm import tqdm
import dotenv; dotenv.load_dotenv()

#----------------------

stocks = json.load(open('stocks.json'))

FINHUB_KEY = os.getenv('FINHUB_KEY')
finnhub_client = finnhub.Client(api_key=FINHUB_KEY)

#----------------------

sdate_str = '2019-01-01'
edate_str = '2022-01-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')

date_range = [sdate_str] + [(c + datetime.timedelta(days=1)).strftime('%Y-%m-%d') for c in pd.date_range(sdate_dt, edate_dt, freq='Y')]

#----------------------

news_data = []

for stock in tqdm(stocks.keys()):
    for d in range(len(date_range) - 1):
        resp = finnhub_client.company_news(stock, _from=date_range[d], to=date_range[d+1])
        news_data += resp

pd.DataFrame(data=news_data).to_csv('data/temp/company_news.csv', index=False)
