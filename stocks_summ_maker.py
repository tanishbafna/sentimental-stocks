import os
from time import sleep
from tqdm import tqdm
import requests
import json
import csv
from polygon import RESTClient
from dotenv import load_dotenv
load_dotenv()

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

# Constructing connection with YahooFinance API for fundamentals
def return_fundamentals(ticker, **kwargs):
    return None

def main():
    PolygonKey = os.getenv('POLYGON_KEY')
    output = list()
    with RESTClient(PolygonKey) as client:
        for company in tqdm(stocks_list):
            ticker = company['ticker']
            beta = return_beta(ticker, interval="1d", observations=365)
            try:
                stock_info = client.reference_ticker_details(ticker)
            except:
                stock_obj = [company for company in stocks_list if company['ticker'] == ticker][0]
                stock_obj['name'] = None
                stock_obj['sector'] = None
                stock_obj['beta'] = None
                stock_obj['ceo'] = None
                final_list.append(stock_obj)
                continue
            stock_obj = [company for company in stocks_list if company['ticker'] == ticker][0]
            stock_obj['name'] = stock_info.name
            stock_obj['industry'] = stock_info.industry
            stock_obj['sector'] = stock_info.sector
            stock_obj['beta'] = beta
            stock_obj['ceo'] = stock_info.ceo

            output.append(stock_obj)
            sleep(11)
        return output

if __name__ == '__main__':
    final_list = main()
    with open('stock_details', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(["ticker", "name", "industry", "sector", "beta", "ceo"])
            for item in final_list:
                writer.writerow([item["ticker"], item["name"], item["industry"], item["sector"], item["beta"], item["ceo"]])