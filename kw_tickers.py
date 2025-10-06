import os
from dotenv import load_dotenv
from openai import OpenAI
from supabase_client import supabase
import pandas as pd
from pydantic import BaseModel

#------------- load variables --------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

#------------- functions --------------------------------------------
def dataframe_to_markdown(df: pd.DataFrame) -> str:
    header = "| " + " | ".join(df.columns.astype(str)) + " |"
    separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
    body = "\n".join([
        "| " + " | ".join(map(str, row)) + " |"
        for row in df.values
    ])
    markdown_table = "\n".join([header, separator, body])
    return markdown_table

#------------- get exchanges df --------------------------------------
df_exchanges = pd.read_csv(r"ExchangeCodes.csv")
markdown_exchanges = dataframe_to_markdown(df_exchanges)


response = supabase.rpc("get_new_data_kw_ticker").execute()
data = response.data

client = OpenAI()

class tickerformat(BaseModel):
    ticker: str
    code: str
    source: str

def run_kw_tickers_script():
    for row in data:
        try:
            company = row['full_name']
            ticker = row['ticker']
            exchange = row['exchange']
            response = client.responses.parse(
                model="gpt-5-mini",
                input=[
                    {"role": "system", "content": "You will be passed some information of a company and a markdown table of some stock exchange information. The The ticker that is passed is not nessecarily in the right format. The ticker you return should be without any exchange codes. The usage code you return should correspond to on of the values in the the usage_code column of the exchange information markdown table I provided. The source should also correspond to one of the values of the source column of the exchange information markdown table I provided. Do not add any punctuations."},
                    {
                        "role": "user",
                        "content": f"The company {company} has the ticker {ticker} in the exchange {exchange}. \n Here is the exchange information table: {markdown_exchanges}. Please provide the cleaned ticker as well as the usage_code and source as per the exchange information table.",
                    },
                ],
                text_format=tickerformat,
                #temperature=0
            )

            event = response.output_parsed

            response = (
                supabase.table("kw_tickers")
                .upsert(
                    {
                        "ticker": event.ticker,
                        "full_name": row['full_name'],
                        "code": event.code,
                        "source": event.source,
                        "original_ticker": row['ticker'],
                    },
                    on_conflict="ticker, code, source",
                    ignore_duplicates=True,
                )
                .execute()
            )
            print(response)
        except Exception as e:
            print(f"❌ Skipping ticker: '{row['ticker']}' due to error: {e}")
            continue



