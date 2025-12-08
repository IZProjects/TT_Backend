from DataforSEO_client.DataforSEO_client import RestClient
import os
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import datetime, date, timedelta
# You can download this file from here https://cdn.dataforseo.com/v3/examples/python/python_Client.zip


# ------------- load variables --------------------------------------
load_dotenv()
DataForSEO_login = os.getenv("DataForSEO_login")
DataForSEO_API_KEY = os.getenv("DataForSEO_API_KEY")

client = RestClient(DataForSEO_login, DataForSEO_API_KEY)
post_data = dict()


def get_trend(keyword, time_range):
    """
    :param keyword: (str) eg."popmart"
    :param time_range: (str) past_30_days, past_90_days, past_12_months, past_5_years
    :return: [{'date_from': '2020-06-28', 'date_to': '2020-07-04', 'timestamp': 1593302400, 'values': [6]},...]

    to turn the result into a dataframe: df = pd.DataFrame(data)
    """
    post_data = {
        0: {
            "time_range": time_range,
            "keywords": [keyword]
        }
    }
    response = client.post("https://api.dataforseo.com/v3/keywords_data/google_trends/explore/live", post_data)
    if response["status_code"] == 20000:
        data = response['tasks'][0]['result'][0]['items'][0]['data']
        for item in data:
            if item.get("values") == [None]:
                item["values"] = [0]
        return data
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))


def ss_get_volume(keyword_list):
    """
    :param keyword_list: (list) eg.["popmart", 'jellycat']
    :return: [{'keyword': 'jellycat', 'search_volume': 340121, 'country_distribution': [{'country_iso_code': 'US', 'search_volume': 115908, 'percentage': 34.07845},...]},...]

    to turn the result into a dataframe: df = pd.DataFrame(data)
    """
    post_data = {
        "0": {
            "keywords": keyword_list
        }
    }
    response = client.post("/v3/keywords_data/clickstream_data/global_search_volume/live", post_data)
    if response["status_code"] == 20000:
        result = response['tasks'][0]['result'][0]['items']
        return result
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))

def ss_get_monthly_vol(trend, known_volume):
    """
    :param trend: (list) direct from get_trend()
    :param known_volume: (int)
    :return: (str) 06/01/2024: 10783, 07/01/2024: 77881, 08/01/2024: 94655, 09/01/2024: 94655, 10/01/2024: 150969, ...
    """
    df = pd.DataFrame(trend)
    df['date'] = pd.to_datetime(df['date_from'])
    df['trend'] = df['values'].str[0]

    # STEP 2: Determine last completed calendar month
    today = pd.Timestamp.today()
    first_day_this_month = pd.Timestamp(today.year, today.month, 1)
    last_completed_month = first_day_this_month - pd.DateOffset(days=1)
    last_month_start = pd.Timestamp(last_completed_month.year, last_completed_month.month, 1)
    last_month_end = pd.Timestamp(last_completed_month.year, last_completed_month.month, last_completed_month.days_in_month)


    # STEP 3: Calculate scaling factor based on known volume for last completed month
    last_month_data = df[(df['date'] >= last_month_start) & (df['date'] <= last_month_end)]
    total_trend_last_month = last_month_data['trend'].sum()
    scaling_factor = known_volume / total_trend_last_month

    # STEP 4: Estimate weekly volume
    df['estimated_volume'] = df['trend'] * scaling_factor

    # STEP 5: Create year-month column and aggregate to monthly totals
    df['year_month'] = df['date'].dt.to_period('M')
    monthly_df = df.groupby('year_month')['estimated_volume'].sum().reset_index()
    monthly_df['year_month'] = monthly_df['year_month'].dt.to_timestamp()

    # STEP 6: create string
    volume_str = ", ".join([
        f"{date.strftime('%m/%d/%Y')}: {int(volume)}"
        for date, volume in zip(monthly_df['year_month'], monthly_df['estimated_volume'])
    ])
    return volume_str


# --------------------------------------------------------------------------------------------------------------

date_from = date.today().replace(year=date.today().year - 3)
date_from = date_from.strftime("%Y-%m-%d")
def get_volume(keywords):
    # simple way to set a task
    # based on google keyword planner
    """post_data[len(post_data)] = dict(
        keywords=["popmart", "jellycat"],
    )"""
    post_data = {
        0: {
            "keywords": keywords,
            "search_partners": True,
            "date_from": date_from,
        }
    }
    # POST /v3/keywords_data/google_ads/search_volume/live
    # the full list of possible parameters is available in documentation
    response = client.post("/v3/keywords_data/google_ads/search_volume/live", post_data)
    # you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
    if response["status_code"] == 20000:
        results = response['tasks'][0]['result']
        formatted_data = []
        for keyword_data in results:
            #only append if we have the search vol data
            if keyword_data['monthly_searches'] != None:
                keyword = keyword_data['keyword']
                monthly_entries = keyword_data['monthly_searches']
                monthly_entries = sorted(monthly_entries, key=lambda x: (x['year'], x['month']))
                # Format each entry as MM/DD/YYYY: Volume
                formatted_entries = []
                for entry in monthly_entries:
                    date_str = datetime(entry['year'], entry['month'], 1).strftime('%m/%d/%Y')
                    formatted_entries.append(f"{date_str}: {entry['search_volume']}")

                # Join into a single string
                formatted_string = ', '.join(formatted_entries)

                # Append as dictionary
                formatted_data.append({keyword: formatted_string})
            else:
                pass
        return formatted_data
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))



from datetime import datetime, date
import re
from collections import defaultdict


def gtrend_to_volumes(trends_data, monthly_total_str):
    # Parse monthly total (e.g. "06/01/2025: 50000")
    month_str, total_str = monthly_total_str.split(":")
    month_date = datetime.strptime(month_str.strip(), "%m/%d/%Y")
    monthly_total = int(total_str.strip())
    month_key = month_date.strftime("%Y-%m")  # e.g., "2025-06"

    # Collect relative values for that month
    month_values = []
    for entry in trends_data:
        date = datetime.strptime(entry['date_from'], "%Y-%m-%d")
        if date.strftime("%Y-%m") == month_key:
            month_values.append(entry['values'][0])
    total_relative = sum(month_values)
    if total_relative == 0:
        raise ValueError(f"No relative values found for {month_key}.")

    # Global scaling factor
    scale = monthly_total / total_relative

    # Compute daily volumes for ALL days
    daily_volumes = {}
    for entry in trends_data:
        date = datetime.strptime(entry['date_from'], "%Y-%m-%d").strftime("%Y-%m-%d")
        relative_val = entry['values'][0]
        daily_volumes[date] = int(round(relative_val * scale))

    return daily_volumes


def format_daily_volumes_str(daily_volumes):
    """Return 'MM/DD/YYYY: Volume, ...' sorted chronologically."""
    def parse(d):
        d = d.strip()
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            return datetime.strptime(d, "%m/%d/%Y")
    items = sorted(((parse(k), v) for k, v in daily_volumes.items()), key=lambda x: x[0])
    return ", ".join(f"{d.strftime('%m/%d/%Y')}: {int(v)}" for d, v in items)

def monthly_totals_str(s: str) -> str:
    """
    Parse 'MM/DD/YYYY: Volume, ...' string and return
    'MM/01/YYYY: MonthlyTotal, ...' ordered by month.
    """
    totals = defaultdict(int)

    # Find all (MM, DD, YYYY, Volume) tuples
    for mm, dd, yyyy, vol in re.findall(r'(\d{2})/(\d{2})/(\d{4})\s*:\s*(-?\d+)', s):
        key = (int(yyyy), int(mm))
        totals[key] += int(vol)

    # Build result parts sorted chronologically
    parts = []
    for (yyyy, mm) in sorted(totals):
        first_day = date(yyyy, mm, 1).strftime('%m/%d/%Y')
        parts.append(f"{first_day}: {totals[(yyyy, mm)]}")

    return ", ".join(parts)









def get_SERP_AI(keyword, language_code="en", location_code=2840):
    """
    Fetches Google AI Mode SERP data for a given keyword.

    :param keyword: (str) Search query (e.g., "what is google ai mode")
    :param language_code: (str) ISO language code, default "en"
    :param location_code: (int) Numeric location code, default 2840 (US)
    :return: dict with SERP data if successful, otherwise None
    """

    post_data = {
        "0": {
            "language_code": language_code,
            "location_code": location_code,
            "keyword": keyword
        }
    }

    response = client.post("/v3/serp/google/ai_mode/live/advanced", post_data)

    if response.get("status_code") == 20000:
        print(response)
        return response["tasks"][0]["result"][0]['items'][0]['markdown']
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
        return None


#r = get_SERP_AI("#camskattebo is trending on TikTok after NY Giants RB Cam Skattebo suffered a season-ending dislocated ankle and fibula fracture. His fearless, high-energy persona and post-game celebrations sparked a viral wave of support. Online debate over the tackle (hip-drop) and player safety followed. He had successful surgery and is expected to fully recover for next season, with recovery videos still circulating. I've been told this could have some impact to Microsoft Corporation (MSFT). What is the magnitide (high or low) and direciton (positive or negative) of the impact?")
#print(r)