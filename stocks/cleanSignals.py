import json
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm

pd.options.mode.chained_assignment = None

#----------------------

stocks = json.load(open('stocks.json'))

sdate_str = '2019-01-01'
edate_str = '2021-12-01'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')

price_signals = pd.read_csv('data/temp/price_signals_raw.csv')
price_signals_data = []

#----------------------

for stock in tqdm(stocks.keys()):

    df = price_signals.loc[price_signals.ticker == stock]
    df.rename(columns={'ticker':'Stock', 'date':'Date'}, inplace=True)
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

    df.set_index('Date', inplace=True)
    df = df[sdate_str:'2021-11-30']

    df_na = df.loc[(df.REL_VOL.isna())]
    assert len(df_na.index) == 0

    df['Date'] = df.index
    df.reset_index(drop=True, inplace=True)
    df = df[['Stock', 'Date', 'MAVG_20', 'MAVG_200', 'EMA', 'MACD', 'MACD_EMA', 'REL_VOL']]

    price_signals_data.append(df)

pd.concat(price_signals_data, ignore_index=True).to_csv('data/quantitative/price_signals.csv', index=False) 