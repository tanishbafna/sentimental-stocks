import json
import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

#----------------------

stocks = json.load(open('stocks.json'))

sdate_str = '2019-01-01'
edate_str = '2021-12-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
date_range = pd.date_range(sdate_dt, edate_dt - datetime.timedelta(days=1), freq='d')
bdate_range = pd.bdate_range(sdate_dt, edate_dt - datetime.timedelta(days=1))
weekends_dt = [c for c in date_range.difference(bdate_range)]

holidays_str = ['2019-01-01','2019-01-21','2019-02-18','2019-04-19','2019-05-27','2019-07-04','2019-09-02','2019-11-28','2019-12-25',
    '2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25',
    '2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-12-24']
holidays_dt = [datetime.datetime.strptime(c, '%Y-%m-%d') for c in holidays_str]

nonWorkingDays = weekends_dt + holidays_dt
price_data = []

#----------------------

for stock in tqdm(stocks.keys()):

    ticker = yf.Ticker(stock)
    historical_data = ticker.history(start=sdate_str, end=edate_str)

    historical_data['Stock'] = stock
    historical_data['Date'] = historical_data.index

    historical_data = historical_data[sdate_str:edate_str]
    missing_dates = [date for date in date_range.difference(historical_data.index) if date not in nonWorkingDays]

    try:
        assert len(missing_dates) == 0
    except AssertionError:
        print(f'{stock}: {missing_dates}')
        continue

    historical_data = historical_data[['Stock','Date','Open','Low','High','Close','Volume']]
    historical_data.reset_index(drop=True, inplace=True)

    price_data.append(historical_data)

#----------------------

pd.concat(price_data, ignore_index=True).to_csv('data/quantitative/stock_prices2.csv', index=False)
