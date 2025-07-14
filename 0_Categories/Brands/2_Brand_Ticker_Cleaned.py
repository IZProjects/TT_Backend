import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd
from pydantic import BaseModel
from openai import OpenAI

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

#------------- get companies df --------------------------------------
conn = sqlite3.connect('../../data.db')
df_companies = pd.read_sql('SELECT * FROM Public_Brands', conn)
df_companies = df_companies.dropna()
df_companies = df_companies[
    (df_companies['Ticker'] != 'N/A') &
    (df_companies['Exchange'] != 'N/A')
]
df_companies = df_companies.reset_index(drop=True)

#------------- get exchanges df --------------------------------------
df_exchanges = pd.read_csv(r"../../ExchangeCodes.csv")
markdown_exchanges = dataframe_to_markdown(df_exchanges)

#------------- openai model ------------------------------------------
client = OpenAI()

class tickerformat(BaseModel):
    ticker: str
    code: str
    source: str

rows = []
for i in range(len(df_companies)):
    company = df_companies.at[i,'Company']
    ticker = df_companies.at[i,'Ticker']
    exchange = df_companies.at[i,'Exchange']
    response = client.responses.parse(
        model="gpt-4o-2024-08-06",
        input=[
            {"role": "system", "content": "You have been passed some information of a company and a markdown table of some stock exchange information. Extract the company's keyword, ticker without any exchange information attached, the relevant exchange code for the ticker from the second table and the associated source to use in the second table."},
            {
                "role": "user",
                "content": f"The company {company} has the ticker {ticker} in the exhcnage {exchange}. The ticker is not nessecarily in the form that I want. \n Here is the exchange information: {markdown_exchanges}",
            },
        ],
        text_format=tickerformat,
        temperature=0
    )

    event = response.output_parsed
    rows.append([df_companies.at[i,'Keyword'], df_companies.at[i,'Company'], event.ticker, event.code, event.source])

df = pd.DataFrame(rows, columns=["Keyword", "Company", "Ticker", "Code", "Source"])

#------------- save dataframe ------------------------------------------
df.to_sql('Brand_Ticker_Cleaned', conn, if_exists='replace', index=False)
df.to_csv(r"../../Brand_Ticker_Cleaned.csv")

conn.close()