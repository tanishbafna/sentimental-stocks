import requests
import pandas as pd
import numpy as np
from datetime import date, timedelta
import time
import os
from tqdm import tqdm
from dotenv import load_dotenv; load_dotenv()


alpha_key = os.getenv('ALPHA_KEY1')
base_url = 'https://www.alphavantage.co/query/'
start_date = date(2018,1,1)
end_date = date.today()
tickers = ["TSLA", "BABA", "HD", "NVDA", "JNJ", "JPM", "NKE", "TWTR", "AAPL", "AMZN", "XOM", "AMC", "PLUG", "PG", "PFE"]


def datetime_range(start=None, end=date.today()):
  delta = end - start
  for i in range(delta.days + 1):
    yield start + timedelta(days=i)


def prices_df(ticker, start_date=date.today() - timedelta(days=365), end_date=date.today()):
  params = {'function': 'TIME_SERIES_DAILY', 'symbol': ticker, 'outputsize': 'full', 'apikey': alpha_key}
  resp = requests.get(base_url, params=params)
  data = resp.json()
  data = data['Time Series (Daily)']

  columns=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'ticker', 'date']
  df = pd.DataFrame(columns=columns, index=pd.to_datetime([]))
  # print(data['2021-02-02'])
  for date in list(datetime_range(start=start_date, end=end_date)):
    date = date.strftime('%Y-%m-%d')
    try:
      values = list(data[date].values())
      values.extend([ticker, date])
      df.loc[date] = pd.Series([float(value) if value != ticker and value != date else value for value in values], columns)
    except KeyError:
      continue
  df['date']= pd.to_datetime(df['date'])
  df.set_index(['ticker', 'date'], inplace=True)
  return df


def price_signals(df_prices):

    # Create new DataFrame for the signals.
    # Setting the index improves performance.
    df_signals = pd.DataFrame(index=df_prices.index)

    # df_signals['ticker'] = df_prices['ticker']

    # Moving Average for past 20 days.
    df_signals['MAVG_20'] = df_prices['CLOSE'].rolling(window=20).mean()

    # Moving Average for past 200 days.
    df_signals['MAVG_200'] = df_prices['CLOSE'].rolling(window=200).mean()

    # Exponential Moving Average for past 20 days.
    df_signals['EMA'] = df_prices['CLOSE'].ewm(span=20).mean()
    
    # Moving Average Convergence Divergence for 12 and 26 days.
    # https://en.wikipedia.org/wiki/MACD
    df_signals['MACD'] = df_prices['CLOSE'].ewm(span=12).mean() - df_prices['CLOSE'].ewm(span=26).mean()
    
    # MACD with extra smoothing by Exp. Moving Average for 9 days.
    df_signals['MACD_EMA'] = df_signals['MACD'].ewm(span=9).mean()

    # The last trading volume relative to 20-day moving average.
    df_signals['REL_VOL'] = np.log(df_prices['VOLUME'] / df_prices['VOLUME'].rolling(window=20).mean())
    
    return df_signals


df_prices = prices_df(ticker='JNJ', start_date=start_date)
df_prices.head()

df_signals = price_signals(df_prices=df_prices)
df_signals.head()

final_prices = pd.DataFrame(index=pd.to_datetime([]))
for ticker in tickers:
  print(ticker)
  df_prices = prices_df(ticker, start_date, end_date)
  final_prices = final_prices.append(df_prices)
  time.sleep(9.5)

# final_prices.set_index(['ticker'], inplace=True )
final_prices.head()

final_signals = pd.DataFrame(index=pd.to_datetime([]))
for ticker, df_grp in final_prices.groupby('ticker'):
  df_signals = price_signals(df_prices=df_grp)
  final_signals = final_signals.append(df_signals)


final_signals.to_csv("data/quantitative/price_signals.csv")


def _asfreq(df_grp):

    df_grp = df_grp.reset_index('ticker', drop=True)
    
    # Perform the operation on this group.
    df_result = df_grp.asfreq(freq='D', method='ffill')

    return df_result


def df_income(ticker, start_date=date.today() - timedelta(days=365), end_date=date.today()):
  params = {'function': 'INCOME_STATEMENT', 'symbol': ticker, 'apikey': alpha_key}
  resp = requests.get(base_url, params=params)
  data = resp.json()
  data = data['quarterlyReports']

  # print(data)
  time_period_financials = list()
  for date in list(datetime_range(start=start_date, end=end_date)):
      date = date.strftime('%Y-%m-%d')
      found_value = [income_deets for income_deets in data if income_deets['fiscalDateEnding'] == date]
      if len(found_value) > 0:
        time_period_financials.append(found_value[0])
  
  # Creating dataframe to return
  columns = list(time_period_financials[0].keys())
  df = pd.DataFrame(time_period_financials , columns=columns)
  df['ticker'] = ticker
  df['dateEnding']= pd.to_datetime(df['fiscalDateEnding'])
  df.drop(columns=['fiscalDateEnding', 'reportedCurrency'], inplace=True)
  df.set_index(['ticker', 'dateEnding'], inplace=True)

  # Ensuring all numbers are converted from string to numeric
  for column in df.columns:
    column_vals = pd.to_numeric(df[column], errors='ignore')
    df[column] = column_vals
    
  return df

# Get quarterly balance sheet details for a ticker
def df_balance_sheet(ticker, start_date=date.today() - timedelta(days=365), end_date=date.today()):
  params = {'function': 'BALANCE_SHEET', 'symbol': ticker, 'apikey': alpha_key}
  resp = requests.get(base_url, params=params)
  data = resp.json()
  data = data['quarterlyReports']

  # print(data)
  time_period_financials = list()
  for date in list(datetime_range(start=start_date, end=end_date)):
      date = date.strftime('%Y-%m-%d')
      found_value = [income_deets for income_deets in data if income_deets['fiscalDateEnding'] == date]
      if len(found_value) > 0:
        time_period_financials.append(found_value[0])

  columns = list(time_period_financials[0].keys())
  df = pd.DataFrame(time_period_financials , columns=columns)
  df['ticker'] = ticker
  df['dateEnding']= pd.to_datetime(df['fiscalDateEnding'])
  df.drop(columns=['fiscalDateEnding', 'reportedCurrency'], inplace=True)
  df.set_index(['ticker', 'dateEnding'], inplace=True)

  # Ensuring all numbers are converted from string to numeric
  for column in df.columns:
    column_vals = pd.to_numeric(df[column], errors='ignore')
    df[column] = column_vals

  return df

# Get quarterly cashflow details for a ticker
def df_cashflow(ticker, start_date=date.today() - timedelta(days=365), end_date=date.today()):
  params = {'function': 'CASH_FLOW', 'symbol': ticker, 'apikey': alpha_key}
  resp = requests.get(base_url, params=params)
  data = resp.json()
  data = data['quarterlyReports']

  # print(data)
  time_period_financials = list()
  for date in list(datetime_range(start=start_date, end=end_date)):
      date = date.strftime('%Y-%m-%d')
      found_value = [income_deets for income_deets in data if income_deets['fiscalDateEnding'] == date]
      if len(found_value) > 0:
        time_period_financials.append(found_value[0])

  columns = list(time_period_financials[0].keys())
  df = pd.DataFrame(time_period_financials , columns=columns)
  df['ticker'] = ticker
  df['dateEnding']= pd.to_datetime(df['fiscalDateEnding'])
  df.drop(columns=['fiscalDateEnding', 'reportedCurrency'], inplace=True)
  # pd.to_numeric(df,)
  df.set_index(['ticker', 'dateEnding'], inplace=True)
  
  # Ensuring all numbers are converted from string to numeric
  for column in df.columns:
    column_vals = pd.to_numeric(df[column], errors='ignore')
    df[column] = column_vals
  
  return df


apple_cashflow_df = df_cashflow('AAPL')

income_df = pd.DataFrame(index=pd.to_datetime([]))
balance_df = pd.DataFrame(index=pd.to_datetime([]))
cashflow_df = pd.DataFrame(index=pd.to_datetime([]))
time.sleep(40)
for ticker in tqdm(tickers):
  # print(ticker)
  # Quarterly Income
  alpha_key = os.getenv('ALPHA_KEY1')
  ticker_income = df_income(ticker, start_date, end_date)
  # Quarterly Balance Sheet
  alpha_key = os.getenv('ALPHA_KEY2')
  ticker_balance_sheet = df_balance_sheet(ticker, start_date, end_date)
  # Quarterly Cashflow
  alpha_key = os.getenv('ALPHA_KEY3')
  ticker_cashflow = df_cashflow(ticker, start_date, end_date)

  # adding to final dataframes
  income_df = income_df.append(ticker_income)
  balance_df = balance_df.append(ticker_balance_sheet)
  cashflow_df = cashflow_df.append(ticker_cashflow)
  time.sleep(11)

income_df2 = income_df.groupby('ticker').apply(_asfreq)
balance_df2 = balance_df.groupby('ticker').apply(_asfreq)
cashflow_df2 = cashflow_df.groupby('ticker').apply(_asfreq)

income_df2.to_csv("data/quantitative/income_statements.csv")
balance_df2.to_csv("data/quantitative/balance_sheets.csv")
cashflow_df2.to_csv("data/quantitative/cashflows.csv")

def fin_signals(income_df, balance_df):

    # Create new DataFrame for the signals.
    # Setting the index improves performance.
    df_signals = pd.DataFrame(index=income_df.index)

    # Quarterly year-over-year sales-growth 
    df_signals['SALES_GROWTH'] = income_df['totalRevenue'] / income_df['totalRevenue'].shift(4) - 1

    # Net Profit Margin.
    df_signals['NET_PROFIT_MARGIN'] = income_df['netIncome'] / income_df['totalRevenue']
    
    # Return on Assets.
    df_signals['ROA'] = income_df['netIncome'] / balance_df['totalAssets'].shift(4)
    
    # Return on Equity.
    df_signals['ROE'] = income_df['netIncome'] / balance_df['totalShareholderEquity'].shift(4)

    return df_signals

fin_signals(income_df.loc['AAPL'], balance_df.loc['AAPL'])

def coerce_num(df):
  df = df.copy()
  for column in df.columns:
      column_vals = pd.to_numeric(df[column], errors='coerce')
      df[column] = column_vals
  return df

income_df3 = coerce_num(income_df)
balance_df3 = coerce_num(balance_df)
cashflow_df3 = coerce_num(cashflow_df)

all_fin_signals = pd.DataFrame(index=pd.to_datetime([]))
for ticker, df_grp in income_df3.groupby('ticker'):
  print(ticker)
  df_fin_signal = fin_signals(income_df=df_grp, balance_df=balance_df3.loc[ticker])
  all_fin_signals = all_fin_signals.append(df_fin_signal)

final_fin_signals = all_fin_signals.groupby('ticker').apply(_asfreq)
final_fin_signals.to_csv("data/quantitative/financial_signals.csv")
