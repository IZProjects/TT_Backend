from utils.EODHD_functions import get_historical_stock_data, get_weekly_data
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import sqlite3
import yfinance as yf
from supabase_client import supabase

# ------------------------------------------------ functions ----------------------------------------------------------
def get_weekly_data_yf(df):
    # Ensure datetime index
    df['Date'] = pd.to_datetime(df.index)

    # Calculate start-of-week (Monday) for each row
    df['week_start'] = df['Date'] - pd.to_timedelta(df['Date'].dt.weekday, unit='d')

    # Group by week start and aggregate
    weekly_df = (
        df.groupby('week_start').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).reset_index()
    )

    return weekly_df


def run_kw_stock_price_script():
    # -------------------------------------------------- get data ---------------------------------------------------------
    response = (
        supabase.table("kw_tickers")
        .select("*")
        .execute()
    )
    data = response.data

    # -------------------------------------------------- get stock price --------------------------------------------------
    from_date = (datetime.today() - relativedelta(years=3)).strftime('%Y-%m-%d')
    to_date = datetime.today().strftime('%Y-%m-%d')

    for row in data:
        try:
            source = row['source']
            if source == "EODHD":
                # -------------------------------------------------- get EODHD --------------------------------------------
                df = get_historical_stock_data(f"{row['ticker']}.{row['code']}", from_date, to_date)
                df = get_weekly_data(df)
                df = df.drop(columns=['adjusted_close'])
            else:
                # -------------------------------------------------- get yfinance -----------------------------------------
                df = yf.Ticker(f"{row['ticker']}.{row['code']}").history(period="3y")
                df = get_weekly_data_yf(df)
                df.columns = df.columns.str.lower()

            price_history = ", ".join(
                f"{d.strftime('%m/%d/%Y')}: {c:.2f}"
                for d, c in zip(df['week_start'], df['close'])
            )
            if price_history is not None:
                #print({f"ticker: {row['ticker']}.{row['code']}, source: {source}, {price_history}"})

                # -------------------------------------------------- save to DB -----------------------------------------------
                response = (
                    supabase.table("kw_tickers")
                    .upsert(
                        {
                            "ticker": row['ticker'],
                            "code": row['code'],
                            "source": row['source'],
                            "original_ticker": row['original_ticker'],
                            "price_history": price_history,

                        },
                        on_conflict="ticker, code, source",
                    )
                    .execute()
                )

        except Exception as e:
            print(f"{row['ticker']}.{row['code']} for {source} failed: {e}")

    response = supabase.table("kw_tickers").delete().is_("price_history", None).execute()
