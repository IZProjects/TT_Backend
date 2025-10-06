import requests
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()
api_key = os.getenv("EODHD_API_KEY")
pd.options.mode.chained_assignment = None  # default='warn'

def get_historical_stock_data(ticker, from_date, to_date):
    # Construct the URL with function inputs
    url = f'https://eodhd.com/api/eod/{ticker}?from={from_date}&to={to_date}&period=d&api_token={api_key}&fmt=json'

    # Make the API request and get the response data
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame.from_dict(data)
        return df
    else:
        return f"Error: {response.status_code} - {response.text}"

def get_weekly_data(df):
    df['date'] = pd.to_datetime(df['date'])
    df['week_start'] = df['date'] - pd.to_timedelta(df['date'].dt.weekday, unit='d')
    weekly_df = (
        df.groupby('week_start').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'adjusted_close': 'last',
            'volume': 'sum'
        }).reset_index()
    )
    return weekly_df

def get_monthly_data(df):
  df['date'] = pd.to_datetime(df['date'])
  df = df.set_index('date', inplace=False)
  monthly_df = df.resample('ME').agg({
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'adjusted_close': 'last',
    'volume': 'sum'
  }).reset_index()
  return monthly_df

def get_real_time_stock_data(ticker):
  url = f'https://eodhd.com/api/real-time/{ticker}?api_token={api_key}&fmt=json'
  response = requests.get(url)
  if response.status_code == 200:
      data = response.json()
      #df = pd.DataFrame.from_dict([data])
      return data
  else:
      return f"Error: {response.status_code} - {response.text}"


def get_real_time_multi_stock_data(tickers):
    #15-20 tickers at a time
    if isinstance(tickers, list):
        tickers_str = ','.join(tickers)
    else:
        tickers_str = tickers  # Support single tickers as well
    url = f'https://eodhd.com/api/real-time/{tickers_str}?api_token={api_key}&fmt=json'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return f"Error: {response.status_code} - {response.text}"

def get_exhanges():
  url = f'https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json'
  response = requests.get(url)
  if response.status_code == 200:
      data = response.json()
      df = pd.DataFrame.from_dict(data)
      return df
  else:
      return f"Error: {response.status_code} - {response.text}"

def get_tickers(exchange_code):
  #US, AU,
  url = f'https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json'
  response = requests.get(url)
  if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame.from_dict(data)
    return df
  else:
      return f"Error: {response.status_code} - {response.text}"

#df = get_exhanges()
#df.to_csv("EODHD_Exchanges.csv")



"""from datetime import datetime
from dateutil.relativedelta import relativedelta
from_date = (datetime.today() - relativedelta(years=3)).strftime('%Y-%m-%d')
to_date = datetime.today().strftime('%Y-%m-%d')
df = get_historical_stock_data("SNROF.US", from_date, to_date)
dfw = get_weekly_data(df)
with pd.option_context(
    "display.max_rows", None,
    "display.max_columns", None,
    "display.width", None,
    "display.max_colwidth", None
):
    print(df)
    print("\n-----------------------------------------------------------------------------------------------------\n")
    print(dfw)
"""
"""from datetime import datetime
from dateutil.relativedelta import relativedelta
from_date = (datetime.today() - relativedelta(years=3)).strftime('%Y-%m-%d')
to_date = datetime.today().strftime('%Y-%m-%d')
df = get_historical_stock_data(f"SWY.US", from_date, to_date)
print(df)"""