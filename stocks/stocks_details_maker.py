import os
import time
import json
import csv
import datetime as dt
import pandas as pd

from pandas.core.indexes.base import Index
import requests
from tqdm import tqdm

from polygon import RESTClient
from dotenv import load_dotenv; load_dotenv()

def epoch_to_frmtdate(epoch_no, format):
        formatted_date = dt.datetime.utcfromtimestamp(epoch_no).strftime(format)
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
        print(f"Error retrieving info from YF API for ticker: {ticker}")
        quit
    
    resp_results = resp['chart']['result'][0]
    # Timestamp management
    format = "%Y/%m/%d" 
    timestamps = resp_results['timestamp']
    final_timestamps = list()
    for time in timestamps:
        final_timestamps.append(epoch_to_frmtdate(time, format=format))
    # Stock quote management
    quotes = resp_results['indicators']['quote'][0]
    data = {
        "date": pd.to_datetime(final_timestamps, format=format),
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
    cwd = os.getcwd()
    with open('./stocks.json', 'r') as f: 
        stocks_file = json.load(f)
        stocks_list = [ticker[0] for ticker in stocks_file.items()]

    main_df = pd.DataFrame()
    for stock in tqdm(stocks_list):
        prices_df = return_historical_prices(stock, range="5y")
        main_df = main_df.append(prices_df) 
    
    main_df.to_csv(os.path.join("stocks", "stock_prices"))

    # with open('stock_details', 'w') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["ticker", "name", "industry", "sector", "beta", "ceo"])
    #         for item in final_list:
    #             writer.writerow([item["ticker"], item["name"], item["industry"], item["sector"], item["beta"], item["ceo"]])