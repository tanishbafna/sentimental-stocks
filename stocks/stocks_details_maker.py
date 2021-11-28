import os
import time
from pandas.core.indexes.base import Index
import requests
import json
import csv
import datetime as dt
import pandas as pd
from tqdm import tqdm
from polygon import RESTClient
from dotenv import load_dotenv, main; load_dotenv()

stocks_list = [
    {
        "industry": "Automobile",
        "ticker": "TSLA",
        "beta": 1.85
    },
    {
        "industry": None,
        "ticker": "HD",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "BABA",
        "beta": 0.62
    },
    {
        "industry": None,
        "ticker": "NVDA",
        "beta": 1.72
    },
    {
        "industry": None,
        "ticker": "JNJ",
        "beta": 0.46
    },
    {
        "industry": None,
        "ticker": "NKE",
        "beta": 0.97
    },
    {
        "industry": None,
        "ticker": "TWTR",
        "beta": 1.47
    },
    {
        "industry": None,
        "ticker": "AAPL",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "AMZN",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "XOM",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "JPM",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "AMC",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "PLUG",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "PG",
        "beta": None
    },
    {
        "industry": None,
        "ticker": "PFE",
        "beta": None
    }
]

def epoch_to_frmtdate(epoch_no):
        formatted_date = dt.datetime.utcfromtimestamp(epoch_no).strftime("%Y/%m/%d")
        return formatted_date

def return_beta(ticker, interval="1d", observations=100):
    base_url = "https://api.newtonanalytics.com/stock-beta/"
    params = {
        "ticker": ticker,
        "index": "^GSPC",
        "interval": interval,
        "observations": observations
    }
    try:
        response = requests.get(base_url, params=params)
    except:
        print(f"Error retrieving beta details response")
    
    response = json.loads(response.text)
    if response['status'] == "200":
        return round(float(response['data']), 2)

# Connection with Yahoo Finance API for historical prices
def return_historical_prices(ticker, range="1y", region="US", interval="1d"):
    base_url = f"https://yfapi.net/v8/finance/chart/{ticker}"
    headers = {"accept": "application/json", "X-API-KEY": os.getenv('YAHOO_FINANCE_KEY')}
    params = {
        "range": range, 
        "region": region,
        "interval": interval,
        "lang": "en"
    }
    try:
        resp = json.loads(requests.get(base_url, params=params, headers=headers).text)
    except: 
        print("Error retrieving info from YF API")
        quit
    print(resp.items())
    resp_results = resp['chart']['result'][0]
    # Timestamp management
    timestamps = resp_results['timestamp']
    final_timestamps = list()
    for time in timestamps:
        final_timestamps.append(epoch_to_frmtdate(time))
    # Stock quote management
    quotes = resp_results['indicators']['quote'][0]
    data = {
        "date": pd.to_datetime(final_timestamps, format="%Y/%m/%d"),
        "ticker": ticker,
        "volume": quotes['volume'],
        "open": quotes['open'],
        "close": quotes['close'],
        "high": quotes['high'],
        "low": quotes['low']
    }
    # Stock Dataframe
    stock_df = pd.DataFrame(data)
    return stock_df

def polygon_summarize(ticker):
    PolygonKey = os.getenv('POLYGON_KEY')
    with RESTClient(PolygonKey) as client:
        beta = return_beta(ticker, interval="1d", observations=365)
        try:
            stock_info = client.reference_ticker_details(ticker)
        except:
            return None
        data = {
            'ticker': ticker,
            'name': stock_info.name,
            'industry': stock_info.industry,
            'sector': stock_info.sector,
            'beta_1y': beta,
            'ceo': stock_info.ceo
        }
    return pd.DataFrame(data)

if __name__ == '__main__':
    prices_df = pd.DataFrame()
    for stock in tqdm(stocks_list):
        prices_df = return_historical_prices(stock['ticker'], range="2y")
        main_df = main_df.append(prices_df) 
    
    prices_df.to_csv("FTest")

    # with open('stock_details', 'w') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["ticker", "name", "industry", "sector", "beta", "ceo"])
    #         for item in final_list:
    #             writer.writerow([item["ticker"], item["name"], item["industry"], item["sector"], item["beta"], item["ceo"]])