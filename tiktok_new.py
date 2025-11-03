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




def trend_what_and_why(hashtag):
    return get_SERP_AI(f"#{hashtag} is trending on tiktok. what is it and why is it trending")

def summarise_trend(Q):
    S = ("You will be given some text describing what something is and why it is trending. Please summarise it in less "
         "than 500 characters.")
    return ask_gpt(query=Q, system_prompt=S)

def does_trend_have_stocks(hashtag, summary):
    return get_SERP_AI(f"#{hashtag} is trending on tiktok. Are there any publicly traded companies that are strongly "
                       f"affiliated with this trend? Give me only the most relevant companies and how they are related "
                       f"to {hashtag}. Here is some context as to what #{hashtag} is and why it is trending: "
                       f"{summary}")
def extract_companies_or_None(Q):
    S = ("You will be provided with some text. If there are any publicly traded companies in the text "
          "please extract the names of all the companies in a markdown list. If there are none, return 'none' without "
         "any other text or punctuation.")
    return ask_gpt(query=Q, system_prompt=S)

def get_ticker_exhcnage_country(companies):
    return get_SERP_AI(f"Can you please give me the give me the full name of the stock, the stock ticker, the exchange "
                       f"the stock is in, the country the exchange is in for these public traded companies "
                       f"{companies}.")

def get_markdown_tbl(text):
    S = (f"You will be provided some information of some companies. For all the public companies, create a markdown "
         f"table with the following columns: 'Full Name', 'Ticker', 'Exchange', 'Country'. Fill any columns or rows "
         f"you don't know with NA. Do not provide anything other than the markdown table.")
    return ask_gpt(query=text, system_prompt=S)


def add_relationship_to_tbl(hashtag, markdown, relationship):
    S = (f"You will be provided with a markdown table of companies and some text on how these companies relate to the "
         f"{hashtag}. Add the relationship into the markdown table in a new column named Relation")
    Q = f"Markdown company table: {markdown}, relationship text: {relationship}"
    return ask_gpt(query=Q, system_prompt=S)


def add_impact_and_direction(df, hashtag, trendSummary):
    magnitude = []
    direction = []
    for i in range(len(df)):
        company = df.at[i,'Full Name']
        relation = df.at[i,'Relation']
        S = (f"You will be given a company, a trending hashtag, the reason the hashtag is trending and the "
              f"relationship between the company and the trend. Your job is to provide 2 labels informing if the trend "
              f"will have a big impact on the whole company's future financial performance. Consider the size of the "
              f"company and its core businesses. Your first label is to describe the magnitude of the impact eg "
              f"'High', 'Low', 'No Impact' and the second label the direction of the impact eg 'Positive', 'Negative' "
              f"or 'Neutral'. Only provide me with the labels separated by a comma.")
        Q = f"company: {company}, hashtag: {hashtag}, reason for trend: {trendSummary}, relationship: {relation}"
        A = ask_gpt(query=Q, system_prompt=S)
        a, b = A.split(",")
        magnitude.append(a)
        direction.append(b)
    df["Impact"] = magnitude
    df["Direction"] = direction
    return df

def check_data_availability(ticker, code):
    df_price = get_data(f"{ticker}.{code}")
    if not df_price.empty:
        return 'yes'
    else:
        return 'no'

def insert_code(df, exchanges_tbl, output_format):
    data = []
    for index, row in df.iterrows():
        S = ("You will be passed some information of a company and a markdown table of some stock exchange "
              "information. The The ticker that is passed is not nessecarily in the right format. The ticker "
              "you return should be without any exchange codes. The usage code you return should correspond to "
              "on of the values in the the usage_code column of the exchange information markdown table I "
              "provided. Do not add any punctuations. If you don't know, insert the code as NA")

        Q = (f"The company {row['Full Name']} has the ticker {row['Ticker']} in the exchange {row['Exchange']}. "
              f"\n Here is the exchange information table: {exchanges_tbl}. Please provide the cleaned "
              f"ticker as well as the usage_code as per the exchange information table.")

        A = ask_gpt_formatted(query=S, system_prompt=Q, model="gpt-5-mini",
                                                 output_format=output_format)
        ticker = A.ticker
        code = A.code

        check = check_data_availability(ticker, code)

        if check == 'yes':
            stock_data = {"ticker": ticker,
                          "full_name": row['Full Name'],
                          "code": code,
                          "exchange": row['Exchange'],
                          "country": row['Country'],
                          "relation": row['Relation'],
                          "impact": row['Impact'],
                          "direction": row['Direction'],
                          }

            data.append(stock_data)

    return data


def parse_appify_data(apify):
    allHastags = []
    for i in range(len(apify)):
        try:
            # ------------------------------------------- parse appify 3 yr data ----------------------------------
            data = apify[i]
            hashtag_name = data['hashtag_name']
            trend = data['analytics']['trend']
            formatted_trend = [f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: {item['value']}"
                               for item in trend]

            ages = data['analytics']['audience_ages_readable']
            formatted_ages = [f"{item['age_range']}: {item['score']}" for item in ages]
            ages_string = ', '.join(formatted_ages)

            views = str(data['analytics']['video_views'])
            posts = str(data['analytics']['publish_cnt'])
            views_all = str(data['analytics']['video_views_all'])
            posts_all = str(data['analytics']['publish_cnt_all'])

            trend_views = trends_to_actual(formatted_trend, int(views))
            trend_string = ', '.join(trend_views)
            past_trend, proj_trend = split_last_pair(trend_string)

            countries = data['analytics']['audience_countries']
            formatted_countries = [f"{item['country_info']['value']}: {item['score']}" for item in countries]
            countries_string = ', '.join(formatted_countries)

            category = data['analytics']['industry_info']['value']

            related_hashtags = data['analytics']['related_hashtags']
            formatted_hashtags = [item['hashtag_name'] for item in related_hashtags]
            hashtags_string = ', '.join(formatted_hashtags)

            singleDict = {
                "hashtag": hashtag_name,
                "trend": past_trend,
                "ages": ages_string,
                "views_3y": views,
                "posts_3y": posts,
                "views_all": views_all,
                "posts_all": posts_all,
                "countries": countries_string,
                "categories": category,
                "related_hashtag": hashtags_string,
                "trend_projected": proj_trend,
                "created_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat()
            }

            allHastags.append(singleDict)
        except Exception as e:
            print(f"❌ Skipping keyword '{apify[i]['hashtag_name']}' due to error: {e}")
            continue

    return allHastags



response = (
    supabase.table("tiktok2")
    .select("hashtag")
    .execute()
)
existing = [item["hashtag"] for item in response.data]

# ------------------------------------------- Appify scraping ---------------------------------------------------------

apify = tiktok_top100_with_analytics(analytics_period="1095", type="top100_with_analytics", new=True,
                                        period="30", industry="0", country="ALL")

allHastags = parse_appify_data(apify)
# remove any duplicate hashtags
allHastags = list({d['hashtag']: d for d in allHastags}.values())
# remove existing hashtags
newHastags = [d for d in allHastags if d.get("hashtag") not in existing]

# --------------------------------------- filter for relevance and get stock info -------------------------------------

# get exchange codes
df_exchanges = pd.read_csv(r"ExchangeCodes.csv")
markdown_exchanges = dataframe_to_markdown(df_exchanges)

# ticker, code format for AI
class tickerformat(BaseModel):
    ticker: str
    code: str


data = []
for dictionary in newHastags:
    hashtag = dictionary['hashtag']
    trendInfo = trend_what_and_why(hashtag)
    trendSummary = summarise_trend(trendInfo)
    stockTest = does_trend_have_stocks(hashtag, trendSummary)
    stockResponse = extract_companies_or_None(stockTest)

    if stockResponse.lower() != "none":
        stockInfo = get_ticker_exhcnage_country(stockResponse)
        markdown = get_markdown_tbl(stockInfo)
        markdown2 = add_relationship_to_tbl(hashtag, markdown, stockTest)

        df = markdown_to_df(markdown2)
        df = df.dropna().reset_index(drop=True)
        df = df.astype(str)
        df = df[~df.isin(["NA"]).any(axis=1)].reset_index(drop=True)
        df = add_impact_and_direction(df, hashtag, trendSummary)

        data = insert_code(df, markdown_exchanges, tickerformat)
    else:
        print("No stocks")

    if len(data) > 0:
        new_dict = dictionary | {'stocks': data, 'description':trendSummary}
        response = (
            supabase.table("tiktok2").upsert(new_dict, on_conflict="hashtag").execute()
        )

