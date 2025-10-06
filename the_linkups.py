import pandas as pd
from utils.Linkup_functions import linkup_query, markdown_to_df
from supabase_client import supabase
import os
from dotenv import load_dotenv
from openai import OpenAI

# ------------------------------------------- set up OpenAI -----------------------------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

# ------------------------------------------- OpenAI Query Function ---------------------------------------------------
def ask_gpt(query):
    response = client.responses.create(
        model="gpt-5-nano",
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You will be provided with a keyword. Your job is to determine if the keyword is "
                                "referring to a specific product or brand or if it is referring to something more "
                                "conceptual like an idea or theme. If it is referring to a product or brand, respond "
                                "with yes. Otherwise respond with no. Only respond with yes or no in lower case with "
                                "no punctuations"
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": query
                    }
                ]
            }
        ],
        #temperature=0,
    )
    result = response.output[1].content[0].text
    return result

def run_linkup_sript(keywords_list):
    # ---------------------------------------- get keywords ---------------------------------------------------------------
    response = (
        supabase.table("q1")
        .select("keyword")
        .execute()
    )
    existing = [item["keyword"] for item in response.data]


    """keywords_list = ['Labubu blind box', 'Popmart', 'Miniso blind box', 'Pdrn toner', 'Sanrio blind box',
                'PassportCard', 'Jellycat', 'Sydney Sweeney', 'wellio', 'American Eagle', 'Well people',
                'non-profit public benefit organization', 'Booktok']"""

    keywords_new = [item for item in keywords_list if item not in existing]

    keywords = [kw for kw in keywords_new if ask_gpt(kw).strip().lower() == 'yes']


    # ---------------------------------------- start linkup process -------------------------------------------------------
    for i in range(len(keywords)):
        try:
            # ---------------------------------------- Q1: Description---------------------------------------------------------
            Q1 = f"{keywords[i]} has been trending . Can you tell me why it is trending?"
            A1 = linkup_query(Q1)
            # ---------------------------------------- Q2: Brand/Product-------------------------------------------------------
            Q2 = (f"{A1} Is the product or brand in this trend owned by a company? If the answer is Yes, "
                  f"Please return a markdown table with the columns 'Yes/No', 'Product/Brand Name', 'Is Brand Yes/No', "
                  f"'Company Name'. If your answer is No, please return the same table with NA in each field."
                  f"Fill columns you don't know with 'NA'.")
            A2 = linkup_query(Q2)
            df_A2 = markdown_to_df(A2)
            df_A2 = df_A2.dropna().reset_index(drop=True)
            df_A2 = df_A2.astype(str)
            df_A2 = df_A2[~df_A2.isin(["NA"]).any(axis=1)].reset_index(drop=True)
            #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                #print(df_A2)

            # ---------------------------------------- Q3: Stock Info ---------------------------------------------------------
            df_A3_list = []
            for k in range(len(df_A2)):
                if df_A2.at[k, 'Yes/No'].strip().lower() == "yes":
                    Q3 = (f"Are any stocks strongly affiliated to the company {df_A2.at[k, 'Company Name']}? "
                          f"If there are please state Yes and provide me the full name of the stock, the stock ticker without any exchange codes, "
                          f"the exchange the stock is in and the company and the country the exchange is in. Return you answer as "
                          f"a markdown table with the columns 'Yes/No', 'Full Name', 'Ticker', 'Exchange', 'Country'. Fill columns "
                          f"you don't know with 'NA'.")
                    A3 = linkup_query(Q3)
                    df_A3 = markdown_to_df(A3)
                    df_A3_list.append(df_A3)

            df_A3_concat = pd.concat(df_A3_list, ignore_index=True)
            df_A3_concat = df_A3_concat.dropna().reset_index(drop=True)
            df_A3_concat = df_A3_concat.astype(str)
            df_A3_concat = df_A3_concat[~df_A3_concat.isin(["NA"]).any(axis=1)].reset_index(drop=True)

            #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                #print(df_A3_concat)

            # ---------------------------------------- Q4: related hashtags ---------------------------------------------------
            if df_A3_concat["Yes/No"].str.lower().eq("yes").any():
                Q4 = (f"What are the 3 most popular tiktok hashtag for {keywords[i]}? Give me only the hashtags separated "
                      f"with a comma.")
                A4 = linkup_query(Q4)

                # save to supabsae db
                response = (
                    supabase.table("q1")
                    .upsert(
                        {"keyword": keywords[i], "description": A1},
                        on_conflict="keyword",
                    )
                    .execute()
                )

                # ---------------------------------------------- Save to DB ---------------------------------------------------
                for j in range(len(df_A2)):
                    response = (
                        supabase.table("q2")
                        .upsert(
                            {"keyword": keywords[i],
                             "product_brand_name": df_A2.at[j, 'Product/Brand Name'],
                             "is_brand_yes_no": df_A2.at[j, 'Is Brand Yes/No'],
                             "company_name": df_A2.at[j, 'Company Name']
                             },
                            on_conflict="keyword, product_brand_name, company_name",
                        )
                        .execute()
                    )

                for j in range(len(df_A3_concat)):
                    response = (
                        supabase.table("q3")
                        .upsert(
                            {"keyword": keywords[i],
                             "full_name": df_A3_concat.at[j, 'Full Name'],
                             "ticker": df_A3_concat.at[j, 'Ticker'],
                             "exchange": df_A3_concat.at[j, 'Exchange'],
                             "country": df_A3_concat.at[j, 'Country']
                             },
                            on_conflict='keyword, ticker'
                        )
                        .execute()
                    )

                response = (
                    supabase.table("q4")
                    .upsert(
                        {"keyword": keywords[i],
                         "hashtag": A4,
                         },
                        on_conflict="keyword",
                    )
                    .execute()
                )
                #print(response)
        except Exception as e:
            print(f"❌ Skipping keyword '{keywords[i]}' due to error: {e}")
            continue
