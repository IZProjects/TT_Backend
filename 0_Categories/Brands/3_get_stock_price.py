from utils.EODHD_functions import get_historical_stock_data, get_monthly_data
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import sqlite3
import yfinance as yf

#------------- functions ---------------------------------------------
def get_monthly_data_yf(df):
  df['Date'] = pd.to_datetime(df.index)
  df = df.set_index('Date', inplace=False)
  monthly_df = df.resample('ME').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last',
    'Volume': 'sum'
  }).reset_index()
  return monthly_df


#------------- get companies df --------------------------------------
conn = sqlite3.connect('../../data.db')
df_companies = pd.read_sql('SELECT * FROM Brand_Ticker_Cleaned', conn)
df_companies = df_companies.dropna()
df_companies = df_companies.reset_index(drop=True)
df_companies['Stock Price'] = None

#-------------- get stock price ---------------------------------------
from_date = (datetime.today() - relativedelta(years=5)).strftime('%Y-%m-%d')
to_date = datetime.today().strftime('%Y-%m-%d')

for i in range(len(df_companies)):
    try:
        source = df_companies.at[i,'Source']
        if source == "EODHD":
            df = get_historical_stock_data(f"{df_companies.at[i,'Ticker']}.{df_companies.at[i,'Code']}", from_date, to_date)
            df = get_monthly_data(df)
            df = df.drop(columns=['adjusted_close'])
        else:
            df = yf.Ticker(f"{df_companies.at[i,'Ticker']}.{df_companies.at[i,'Code']}").history(period="5y")
            df = get_monthly_data_yf(df)
            df.columns = df.columns.str.lower()

        output = ", ".join(
            f"{d.strftime('%m/%d/%Y')}: {c:.2f}"
            for d, c in zip(df['date'], df['close'])
        )
        df_companies.at[i,'Stock Price'] = output
    except Exception as e:
        print(f"{df_companies.at[i,'Ticker']}.{df_companies.at[i,'Code']} failed: {e}")

df_companies = df_companies.dropna()
df_companies = df_companies.reset_index(drop=True)
df_companies.to_sql('Brand_Stock_Price', conn, if_exists='replace', index=False)
df_companies.to_csv(r"../../Brand_Stock_Price.csv")