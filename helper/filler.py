import json
import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm

pd.options.mode.chained_assignment = None

#----------------------

#stocks = json.load(open('stocks.json'))

sdate_str = '2019-01-01'
edate_str = '2021-12-01'

sdate_str_prev = '2018-12-31'
edate_str_real = '2021-11-30'

sdate_dt = datetime.datetime.strptime(sdate_str, '%Y-%m-%d')
edate_dt = datetime.datetime.strptime(edate_str, '%Y-%m-%d')
date_range = pd.date_range(sdate_dt - datetime.timedelta(days=1), edate_dt - datetime.timedelta(days=1), freq='d')
bdate_range = pd.bdate_range(sdate_dt - datetime.timedelta(days=1), edate_dt - datetime.timedelta(days=1))
weekends_dt = [c for c in date_range.difference(bdate_range)]

holidays_str = ['2019-01-01','2019-01-21','2019-02-18','2019-04-19','2019-05-27','2019-07-04','2019-09-02','2019-11-28','2019-12-25',
    '2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25',
    '2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-12-24']
holidays_dt = [datetime.datetime.strptime(c, '%Y-%m-%d') for c in holidays_str]

#----------------------

price_signals = pd.read_csv('data/quantitative/price_signals_raw.csv')
stocks = {k:None for k in price_signals.ticker.unique()}
price_signals_data = []

for stock in tqdm(stocks.keys()):

    print(stock)
    df = price_signals.loc[price_signals.ticker == stock]

    df['date'] = df['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    df.set_index('date', inplace=True)
    df = df[sdate_str_prev:edate_str_real]

    assert df.iloc[0].name == sdate_dt - datetime.timedelta(days=1)
    assert df.iloc[-1].name == edate_dt - datetime.timedelta(days=1)
    
    df['data'] = 1
    df = df.reindex(date_range, fill_value=None)

    df['ticker'] = stock
    df_na = df.loc[(df.data.isna()) & (df.REL_VOL.isna())]

    for idx, row in df_na.iterrows():

        if idx not in weekends_dt and idx not in holidays_dt:
            print(f'Error: {idx}')
            continue
            
        prevDay = (idx - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        df.at[idx, 'MAVG_20'] = df.loc[prevDay]['MAVG_20']
        df.at[idx, 'MAVG_200'] = df.loc[prevDay]['MAVG_200']
        df.at[idx, 'EMA'] = df.loc[prevDay]['EMA']
        df.at[idx, 'MACD'] = df.loc[prevDay]['MACD']
        df.at[idx, 'MACD_EMA'] = df.loc[prevDay]['MACD_EMA']

    df['Date'] = df.index
    df = df[sdate_str:edate_str_real]
    df.reset_index(drop=True, inplace=True)

    df.rename(columns={'ticker':'Stock'}, inplace=True)
    df = df[['Stock', 'Date', 'MAVG_20', 'MAVG_200', 'EMA', 'MACD', 'MACD_EMA', 'REL_VOL']]

    price_signals_data.append(df)

pd.concat(price_signals_data, ignore_index=True).to_csv('data/quantitative/price_signals_final.csv', index=False)