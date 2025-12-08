from supabase_client import supabase
import pandas as pd
from datetime import datetime

def clean_table(tbl_name, uuid='id', timestamp='created_at'):
    """
    Deletes EMPTY and NULL rows from table
    :param tbl_name: (str) table name
    :return:
    """
    response = supabase.table(tbl_name).select("*").range(0, 1).execute()
    d = response.data[0]
    text_cols = [k for k, v in d.items() if isinstance(v, str) and v.strip() != ""]
    columns = list(d.keys())

    for col in columns:
        response = supabase.table(tbl_name).delete().is_(col, None).execute()
        #if col in text_cols and col != timestamp and col != uuid:
            #response = supabase.table(tbl_name).delete().eq(col, "").execute()


def chunk_list(lst, chunk_size=1000):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def trends_to_actual(trend_data, total_searches):
    """
    Convert Trends relative interest values to actual search/view counts.

    Parameters:
        trend_data (list of str): Entries in the form "MM/DD/YYYY: relative_interest"
        total_searches (int or float): Total searches for the entire period

    Returns:
        list of str: Entries in the form "MM/DD/YYYY: actual_searches"
    """
    # Extract numeric values
    values = [float(entry.split(": ")[1]) for entry in trend_data]
    total_relative = sum(values)

    # Calculate actual searches for each period
    searches_per_period = [
        f"{date}: {round((val / total_relative) * total_searches)}"
        for date, val in [(entry.split(": ")[0], float(entry.split(": ")[1])) for entry in trend_data]
    ]

    return searches_per_period

def split_last_pair(s: str):
    s = s.strip().rstrip(', ')            # tidy trailing comma/space
    if not s:
        return "", ""
    if ', ' in s:
        rest, last = s.rsplit(', ', 1)    # split at the final comma+space
        return rest, last
    else:
        return "", s

def dataframe_to_markdown(df: pd.DataFrame) -> str:
    header = "| " + " | ".join(df.columns.astype(str)) + " |"
    separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    body = "\n".join([
        "| " + " | ".join(map(str, row)) + " |"
        for row in df.values
    ])
    markdown_table = "\n".join([header, separator, body])
    return markdown_table

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

def last_value_and_yoy(trend_string: str, backtrack_size=2):
    """
    :param trend_string: "06/01/2024: 10783, 07/01/2024: 77881, 08/01/2024: 94655, ..."
    :return: (second_last_month_str, value_of_second_last_month, YoY_percent)
             - month string in MM/DD/YYYY
             - value (int)
             - YoY as % (float) or None if unavailable / prev is 0
    """
    # Parse "MM/DD/YYYY: value" pairs
    parts = [p.strip() for p in trend_string.strip().strip(",").split(",") if p.strip()]
    data = {}
    for p in parts:
        date_str, val_str = [x.strip() for x in p.split(":", 1)]
        dt = datetime.strptime(date_str, "%m/%d/%Y").date()
        val = int(val_str.replace(",", ""))
        data[dt] = val

    # Need at least two months to get the second-last
    if len(data) < backtrack_size:
        return None, None, None

    # Second-last month by date
    dates = sorted(data)
    target_dt = dates[-backtrack_size]
    target_val = data[target_dt]

    # YoY for that month
    yoy_dt = target_dt.replace(year=target_dt.year - 1)
    prev_val = data.get(yoy_dt)
    if prev_val in (None, 0):
        yoy = None
    else:
        yoy = (target_val - prev_val) / prev_val * 100.0

    return target_dt.strftime("%m/%d/%Y"), target_val, yoy