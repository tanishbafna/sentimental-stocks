import json
import datetime
import numpy as np
import pandas as pd
import yfinance as yf
from tqdm import tqdm

#----------------------

stocks = json.load(open('stocks.json'))

sdate_str = '2020-01-01'
edate_str = '2021-12-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
date_range = pd.date_range(sdate_dt - datetime.timedelta(days=1), edate_dt - datetime.timedelta(days=1), freq='d')
bdate_range = pd.bdate_range(sdate_dt - datetime.timedelta(days=1), edate_dt - datetime.timedelta(days=1))
weekends_dt = [c for c in date_range.difference(bdate_range)]

holidays_str = ['2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25',
'2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-12-24']
holidays_dt = [datetime.datetime.strptime(c, '%Y-%m-%d') for c in holidays_str]

price_data = []
fundamentals_data = []

#----------------------

for stock in tqdm(stocks.keys()):

    ticker = yf.Ticker(stock)
    historical_data = ticker.history(start=sdate_str, end=edate_str)

    assert historical_data.iloc[0].name == sdate_dt - datetime.timedelta(days=1)
    assert historical_data.iloc[-1].name == edate_dt - datetime.timedelta(days=1)

    historical_data = historical_data.reindex(date_range, fill_value=None)
    historical_data['Working_Day'] = 1
    historical_data_na = historical_data[historical_data['Open'].isna()]

    for idx, row in historical_data_na.iterrows():

        if idx not in weekends_dt and idx not in holidays_dt:
            print(f'Error: {idx}')
        else:
            historical_data.at[idx, 'Working_Day'] = 0
        
        prevDay = (idx - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        prevClose = historical_data.loc[prevDay]['Close']
        assert prevClose not in [0.0, np.NaN]

        historical_data.at[idx, 'Open'] = prevClose
        historical_data.at[idx, 'High'] = prevClose
        historical_data.at[idx, 'Low'] = prevClose
        historical_data.at[idx, 'Close'] = prevClose

        historical_data.at[idx, 'Volume'] = 0.0
        historical_data.at[idx, 'Dividends'] = 0.0
        historical_data.at[idx, 'Stock Splits'] = 0.0

    historical_data = historical_data[sdate_str:edate_str]
    historical_data_na = historical_data[historical_data['Open'].isna()]

    assert len(historical_data_na.index) == 0

    historical_data.rename(columns={'Stock Splits':'Stock_Splits'}, inplace=True)
    historical_data['Date'] = historical_data.index
    historical_data['Stock'] = stock

    historical_data = historical_data[['Stock','Date','Working_Day','Open','Low','High','Close','Volume','Dividends','Stock_Splits']]
    historical_data.reset_index(drop=True, inplace=True)

    price_data.append(historical_data)

#----------------------

pd.concat(price_data, ignore_index=True).to_csv('data/quantitative/stock_prices.csv', index=False)
