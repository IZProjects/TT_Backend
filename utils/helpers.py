from supabase_client import supabase
import pandas as pd

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
        if col in text_cols and col != timestamp and col != uuid:
            response = supabase.table(tbl_name).delete().eq(col, "").execute()


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
