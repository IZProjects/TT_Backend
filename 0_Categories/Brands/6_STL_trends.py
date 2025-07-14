import pandas as pd
import sqlite3
from statsmodels.tsa.seasonal import STL
from datetime import datetime
from sklearn.linear_model import LinearRegression
import numpy as np

def format_to_string(df, col_name):
    """
    :param df: (dataframe)
    :param col_name: (str)
    :return: (str) "06/01/2020: 75509.58, 07/01/2020: 76156.94, 08/01/2020: 76706.49, 09/01/2020: 77164.49,..."
    """
    trend = ', '.join([
        f"{date.strftime('%m/%d/%Y')}: {int(round(value)) if pd.notna(value) and np.isfinite(value) else 'NaN'}"
        for date, value in zip(df.index, df[col_name])
    ])
    return trend

def str_to_df(data_str):
    """
    :param data_str: (str) "06/01/2020: 75509.58, 07/01/2020: 76156.94, 08/01/2020: 76706.49, 09/01/2020: 77164.49,..."
    :return: (dataframe) col = date (datetime), volume (int)
    """
    entries = [item.strip() for item in data_str.split(",")]
    data = [(datetime.strptime(date.strip(), "%m/%d/%Y"), int(value.strip())) for date, value in (entry.split(":") for entry in entries)]
    df = pd.DataFrame(data, columns=["Date", "Volume"])
    df = df.sort_values('Date')
    df.set_index("Date", inplace=True)
    return df

def get_STL(df):
    """
    :param df: cols = date (datetime), volume (int)
    :return: (dataframe) with observed, trend, seasonal & residual
    """
    # STL decomposition
    stl = STL(df['Volume'], period=12)
    res = stl.fit()

    # Create a DataFrame with the decomposition results
    stl_df = pd.DataFrame({
        'Observed': res.observed,
        'Trend': res.trend,
        'Seasonal': res.seasonal,
        'Residual': res.resid
    }, index=df.index)

    stl_df.index = pd.to_datetime(stl_df.index)
    stl_df['Trend_Momentum'] = stl_df['Trend'].diff().fillna(0)
    return stl_df

def is_breakout(df):
    """
    :param df: cols = date (datetime), volume (int)
    :return: (str) Breakout or Established
    """
    recent_data = df.tail(3)
    historical_data = df.iloc[-15:-3]

    # Z-score comparison
    mean = historical_data['Volume'].mean()
    std = historical_data['Volume'].std()
    recent_z_scores = (recent_data['Volume'] - mean) / std

    # Breakout if all recent z-scores > 2 (i.e., >2 std dev above mean)
    breakout = recent_z_scores.all() > 2
    return 'Breakout' if breakout else 'Established'


def is_exploding(df):
    """
    :param df: cols = date (datetime), volume (int)
    :return: (boolean) True or False
    """
    recent_data = df.tail(12)
    historical_data = df.iloc[:-12]

    # Z-score comparison
    mean = historical_data['Volume'].mean()
    std = historical_data['Volume'].std()
    recent_z_scores = (recent_data['Volume'] - mean) / std

    # Breakout if all recent z-scores > 2 (i.e., >2 std dev above mean)
    breakout = (recent_z_scores > 2).sum() >= 10
    return breakout



def is_exponential(df, column='Trend'):
    try:
        # Drop missing or non-positive values (log not defined for zero or negative)
        series = df[column].dropna()
        series = series[series > 0]
        series = series.tail(24)

        # Convert index to numeric values (e.g., 0, 1, 2, ...)
        x = np.arange(len(series)).reshape(-1, 1)
        y = series.values
        log_y = np.log(y)

        # Fit linear regression to log-transformed data
        model = LinearRegression()
        model.fit(x, log_y)
        r_squared = model.score(x, log_y)

        return r_squared > 0.9  # You can adjust this threshold

    except Exception as e:
        print(f"Error: {e}")
        return False


def growth_indicator(df):
    mean = df['Trend_Momentum'].tail(3).mean()
    if mean < 5 and mean > -5:
        return "Stationary"
    elif mean > 0 and df.at[df.index[-1],'Trend_Momentum'] > 0:
        return "Accelerating"
    elif mean > 0 and df.at[df.index[-1], 'Trend_Momentum'] <= 0:
        return "Peaking"
    else:
        return "Declining"



#----------------------------------- Run -----------------------------------------

conn = sqlite3.connect('../../data.db')
df_total = pd.read_sql('SELECT * FROM Brand_Stock_Price_SVol', conn)
df_total = df_total.dropna()
df_total = df_total.reset_index(drop=True)
df_total["Volume Trend"] = None
df_total["Volume Momentum"] = None
df_total["is_breakout"] = None
df_total["Growth Indicator"] = None
df_total["YoY Growth"] = None
df_total["Type"] = "Brand"
df_total["Category"] = "Lifestyle"

for i in range(len(df_total)):
    data_str = df_total.at[i,'Search Volume']
    df = str_to_df(data_str)
    stl_df = get_STL(df)
    df_total.at[i,"Growth Indicator"] = growth_indicator(stl_df)
    df_total.at[i,"is_breakout"] = is_breakout(df)
    df_total.at[i,"Volume Trend"] = format_to_string(stl_df,'Trend')
    df_total.at[i,"Volume Momentum"] = format_to_string(stl_df,'Trend_Momentum')
    df['YoY_Growth'] = df['Volume'].pct_change(periods=12) * 100
    df['YoY_Growth'] = df['YoY_Growth'].fillna(0)
    df_total.at[i, "YoY Growth"] = format_to_string(df, 'YoY_Growth')


df_total = df_total.dropna()
df_total = df_total.reset_index(drop=True)
df_total.to_sql('Brand_Stock_Price_SVol_trend', conn, if_exists='replace', index=False)
df_total.to_csv(r"../../Brand_Stock_Price_SVol_trend.csv")