from DataforSEO_client.DataforSEO_client import RestClient
import os
from dotenv import load_dotenv
import pandas as pd
import time
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
    response = client.post("/v3/keywords_data/dataforseo_trends/explore/live", post_data)
    if response["status_code"] == 20000:
        data = response['tasks'][0]['result'][0]['items'][0]['data']
        return data
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))


def get_volume(keyword_list):
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

def get_monthly_vol(trend, known_volume):
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


print('------------------------------------------------------------------------------------------------------')

def get_volume2(keywords):
    # simple way to set a task
    # based on google keyword planner
    """post_data[len(post_data)] = dict(
        keywords=["popmart", "jellycat"],
    )"""
    post_data = {
        0: {
            "keywords": keywords,
            "search_partners": True,
            "date_from": "2020-01-01",
        }
    }
    # POST /v3/keywords_data/google_ads/search_volume/live
    # the full list of possible parameters is available in documentation
    response = client.post("/v3/keywords_data/google_ads/search_volume/live", post_data)
    # you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
    if response["status_code"] == 20000:
        print(response)
        # do something with result
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))



# simple way to set a task
post_data[len(post_data)] = dict(
    keywords=["popmart"]
)
# POST /v3/keywords_data/google_ads/keywords_for_keywords/live
# the full list of possible parameters is available in documentation
response = client.post("/v3/keywords_data/google_ads/keywords_for_keywords/live", post_data)
# you can find the full list of the response codes here https://docs.dataforseo.com/v3/appendix/errors
if response["status_code"] == 20000:
    print(response)
    # do something with result
else:
    print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))