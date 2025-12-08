import time

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
    return get_SERP_AI(f"#{hashtag} is trending on tiktok. What is it and why is it trending")

def summarise_trend(Q):
    S = """
    You will be given text describing what something is and why it is trending. Your task is to summarise this information
    in fewer than 500 characters.

    Output only the summary with no additional commentary.
    """
    return ask_gpt(query=Q, system_prompt=S)

def does_trend_have_stocks(hashtag, summary):
    return get_SERP_AI(f"#{hashtag} is currently trending on TikTok. Are there any publicly traded companies that may be strongly "
    f"impacted by this trend? Here is some context explaining what #{hashtag} is and why it is trending:\n\n"
    f"{summary}")

def extract_companies_or_None(Q):
    S = """
    You will be given a piece of text. Identify any publicly traded companies mentioned in the text.

    Tasks:
    - If publicly traded companies are present, extract all company names and return them as a markdown list.
    - If no publicly traded companies are mentioned, return only the word 'none'.

    Output rules:
    - Do not include explanations, reasoning, or additional text.
    - Do not add any punctuation beyond what is required for a markdown list.
    """
    return ask_gpt(query=Q, system_prompt=S, model="gpt-5-mini")

def get_ticker_exhcnage_country(companies):
    return get_SERP_AI(f"Please provide the full company name, stock ticker and exchange"
                       f"for the following publicly traded companies: {companies}.")

def get_markdown_tbl(text):
    S = """
    You will be given information about several companies. Your task is to identify all public companies and create a
    markdown table with the following columns: 'Full Name', 'Ticker', 'Exchange'.

    Rules:
    - If any field is missing or cannot be determined, fill it with 'NA'.
    - Do not infer or add information that was not explicitly given.
    - Output only the final markdown table and nothing else.
    """

    return ask_gpt(query=text, system_prompt=S)


def check_data_availability(ticker, code):
    df_price = get_data(f"{ticker}.{code}")
    if not df_price.empty:
        return 'yes'
    else:
        return 'no'

def insert_code(df, exchanges_tbl, output_format):
    data = []
    for index, row in df.iterrows():
        S = """
        You will receive information about a company and a markdown table containing stock exchange information.
        The ticker provided may not be in the correct format.

        Your tasks:
        - Return the corrected ticker symbol with all exchange codes removed.
        - Return the appropriate usage_code based on the values in the 'usage_code' column of the provided markdown table.
        - If you cannot determine the correct usage_code, return "NA".

        Output requirements:
        - Do not add punctuation.
        - Output only the corrected ticker and the usage_code.
        """

        Q = (
            f"The company {row['Full Name']} has the ticker '{row['Ticker']}' on the exchange '{row['Exchange']}'. "
            f"Here is the exchange information table:\n{exchanges_tbl}\n"
            f"Please return the cleaned ticker (with no exchange codes) and the appropriate usage_code based on the table."
        )

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
                          "relation": row['Relation'],
                          "impact": row['Impact'],
                          "direction": row['Direction'],
                          }

            data.append(stock_data)

    return data


def analyse_impact(df, trendSummary):
    class impactFormat(BaseModel):
        impact: str
        direction: str
        relation: str

    impacts = []
    directions = []
    relations = []

    # ---- Row-by-row processing to keep lengths aligned with df ----
    for i in range(len(df)):
        company = df.at[i, 'Full Name']
        ticker = df.at[i, 'Ticker']

        # 1) Get SERP analysis
        try:
            analysis = get_SERP_AI(
                f"{trendSummary}. I've been told this could have some impact to {company} "
                f"({ticker}). What is the magnitude and direction of the impact if there is "
                f"any?"
            )
        except Exception as e:
            print(f"get_SERP_AI failed for row {i}: {e}")
            analysis = None

        # If no analysis, append empty placeholders to keep list lengths == len(df)
        if not analysis:
            impacts.append("")
            directions.append("")
            relations.append("")
            time.sleep(1)
            continue

        # 2) Classify impact/direction/relationship with GPT
        system_prompt = """
        You will receive expert analysis describing the relationship between a trend and a company. Your task is to answer
        three questions based solely on that analysis.

        Answering rules:
        - For Questions 1 and 2, you must choose exactly one option from the lists provided.
        - For Question 3, provide a concise summary suitable for inclusion in a report.

        Questions:
        1. What is the financial impact of this trend on the company?
           Choose one: Very Low, Low, Moderate, High, Very High, or No Impact.
        2. What is the direction of the impact?
           Choose one: Positive, Negative, or Neutral.
        3. What is the relationship between the trend and the company?
           Provide a short, clear summary explaining the relationship.

        Output only your three answers. Do not include explanations, reasoning, or additional commentary.
        """

        try:
            # analysis text is the "query", instructions are the "system_prompt"
            A = ask_gpt_formatted(
                query=analysis,
                system_prompt=system_prompt,
                model="gpt-5-mini",
                output_format=impactFormat
            )
            impacts.append(A.impact or "")
            directions.append(A.direction or "")
            relations.append(A.relation or "")
        except Exception as e:
            print(f"ask_gpt_formatted failed for row {i}: {e}")
            impacts.append("")
            directions.append("")
            relations.append("")

        time.sleep(1)

    # At this point, len(impacts) == len(directions) == len(relations) == len(df)
    df = df.copy()
    df['Impact'] = impacts
    df['Direction'] = directions
    df['Relation'] = relations

    # ---- De-duplicate companies via markdown round-trip ----
    markdown = dataframe_to_markdown(df)

    system_prompt_dedupe = """
    You are given a markdown table containing companies related to a trend. Some companies may appear multiple times
    because they are listed on different exchanges.

    Your tasks:
    - Identify rows that refer to the same underlying company.
    - For each duplicated company, review all values in the 'Impact', 'Direction', and 'Relationship' columns.
      Generate a single, consolidated value for each column based on all provided information, and apply these
      consolidated values to every row for that company.
    - If there are no duplicates, return the table unchanged.

    Output requirements:
    - Return only the final markdown table.
    - Do not include explanations, reasoning, or extra text.
    """

    edited_markdown = ask_gpt(query=markdown, system_prompt=system_prompt_dedupe)
    df_edited = markdown_to_df(edited_markdown)

    return df_edited



def parse_appify_data(apify):
    allHastags = []
    for i in range(len(apify)):
        try:
            # ------------------------------------------- parse appify 3 yr data ----------------------------------
            data = apify[i]

            # REQUIRED (keep strict)
            hashtag_name = data['hashtag_name']
            trend = data['analytics']['trend']
            formatted_trend = [
                f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: {item['value']}"
                for item in trend
            ]

            views = str(data['analytics']['video_views'])
            posts = str(data['analytics']['publish_cnt'])
            views_all = str(data['analytics']['video_views_all'])
            posts_all = str(data['analytics']['publish_cnt_all'])

            trend_views = trends_to_actual(formatted_trend, int(views))
            trend_string = ', '.join(trend_views)
            past_trend, proj_trend = split_last_pair(trend_string)

            # OPTIONAL FIELDS (safe fallbacks)

            # 1. Ages
            ages = data.get('analytics', {}).get('audience_ages_readable', [])
            formatted_ages = [
                f"{item.get('age_range', '')}: {item.get('score', '')}" for item in ages
            ]
            ages_string = ', '.join(formatted_ages)

            # 2. Countries
            countries = data.get('analytics', {}).get('audience_countries', [])
            formatted_countries = [
                f"{item.get('country_info', {}).get('value', '')}: {item.get('score', '')}"
                for item in countries
            ]
            countries_string = ', '.join(formatted_countries)

            # 3. Industry Category
            category = data.get('analytics', {}).get('industry_info', {}).get('value', '')

            # 4. Related Hashtags
            related_hashtags = data.get('analytics', {}).get('related_hashtags', [])
            formatted_hashtags = [item.get('hashtag_name', '') for item in related_hashtags]
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
                "updated_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat()
            }

            allHastags.append(singleDict)

        except Exception as e:
            print(f"❌ Skipping keyword '{apify[i]['hashtag_name']}' due to error: {e}")
            continue

    return allHastags


def tiktok_new(analytics_period, an_type, new, period, industry, country):
    impactOrder = ['Very High','High','Moderate','Low','Very Low']
    impact_map = {
        'Very High': 5,
        'High': 4,
        'Moderate': 3,
        'Low': 2,
        'Very Low': 1,
    }

    response = (
        supabase.table("tiktok2")
        .select("hashtag")
        .execute()
    )
    existing = [item["hashtag"] for item in response.data]

    apify = tiktok_top100_with_analytics(
        analytics_period=analytics_period,
        type=an_type,
        new=new,
        period=period,
        industry=industry,
        country=country
    )

    allHastags = parse_appify_data(apify)
    allHastags = list({d['hashtag']: d for d in allHastags}.values())
    newHastags = [d for d in allHastags if d.get("hashtag") not in existing]

    df_exchanges = pd.read_csv(r"ExchangeCodes.csv")
    markdown_exchanges = dataframe_to_markdown(df_exchanges)

    class tickerformat(BaseModel):
        ticker: str
        code: str

    for dictionary in newHastags:
        try:
            # reset per hashtag
            data = []
            impact_score = None
            impact_counts = None

            hashtag = dictionary['hashtag']
            trendInfo = trend_what_and_why(hashtag)
            trendSummary = summarise_trend(trendInfo)
            stockTest = does_trend_have_stocks(hashtag, trendSummary)
            stockResponse = extract_companies_or_None(stockTest)

            if stockResponse.lower() != "none":
                stockInfo = get_ticker_exhcnage_country(stockResponse)
                markdown = get_markdown_tbl(stockInfo)

                df = markdown_to_df(markdown)
                df = df.dropna().reset_index(drop=True)
                df = df.astype(str)
                df = df[~df.isin(["NA"]).any(axis=1)].reset_index(drop=True)
                df = analyse_impact(df, trendSummary)

                # keep only rows with valid impact labels
                df = df[df['Impact'].isin(impact_map.keys())]

                if not df.empty:
                    # numeric impact
                    df['Impact_num'] = df['Impact'].map(impact_map).astype(float)

                    # impact score as plain Python float
                    impact_score = float(df['Impact_num'].mean())

                    # counts as plain Python ints
                    counts = df['Impact'].value_counts()
                    impact_counts = {
                        k: int(counts.get(k, 0)) for k in impactOrder
                    }

                    # optional: ordered categorical for sorting / display
                    df['Impact'] = pd.Categorical(df['Impact'],
                                                  categories=impactOrder,
                                                  ordered=True)
                    df = df.sort_values('Impact').reset_index(drop=True)

                    data = insert_code(df, markdown_exchanges, tickerformat)
            else:
                print("No stocks")

            # only upsert if we actually have stock data
            if len(data) > 0:
                new_dict = dictionary | {
                    'stocks': data,
                    'description': trendSummary,
                    'impact_score': impact_score,      # now a plain float
                    'impact_counts': impact_counts     # now plain ints
                }

                supabase.table("tiktok2").upsert(
                    new_dict, on_conflict="hashtag"
                ).execute()

        except Exception as e:
            print(f"Error in the main loop {e}")

# [analytics_period, type, new, period, industry, country]
apify_inputs = [["1095", "top100_with_analytics", False, "30", "15000000000", "ALL"]]
for l in apify_inputs:
    tiktok_new(l[0], l[1], l[2], l[3], l[4], l[5])