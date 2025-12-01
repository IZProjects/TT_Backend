from utils.Apify_functions import tiktok_top100_with_analytics
from utils.OpenAI_functions import ask_gpt, ask_gpt_formatted
from utils.Linkup_functions import markdown_to_df
from utils.EODHD_functions import get_data
from utils.helpers import trends_to_actual, split_last_pair, dataframe_to_markdown
from utils.DataforSEO_functions import get_SERP_AI
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from pydantic import BaseModel
from supabase_client import supabase


def first_filter(trend, company, ticker, relationship):
    S = (f"You will be provided with a trend, a company and the relationship between the two. Will the trend have a "
         f"big impact on the company? Answer with Yes or No.")
    Q = f"trend: {trend}, company: {company} ({ticker}),relationship: {relationship}"
    return ask_gpt(query=Q, system_prompt=S)

response = (
    supabase.table("tiktok2")
    .select("hashtag, description, stocks")
    .execute()
)
data = response.data
for row in data:
    for stock in row['stocks']:
        a = first_filter(row['description'], stock['full_name'], stock['ticker'], stock['relation'])
        print(row['hashtag'], stock['full_name'], a)
