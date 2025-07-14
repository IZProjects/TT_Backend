import pandas as pd
from linkup import LinkupClient
from dotenv import load_dotenv
import os
from io import StringIO


# ------------------ load API keys --------------------------------
load_dotenv()
api_key = os.getenv("LINKUP_API_KEY")


# ------------------ load data ------------------------------------
df = pd.read_csv(r"C:\00 App Projects\TradingTrends\TT_Backend\data\lifestyle_5yr.csv")
df = df[(df['Keyword'] == 'Popmart') & (df['Brand'] == 'Yes')]
df = df.reset_index(drop=True)


# ------------------ Linkup AI Search -----------------------------

client = LinkupClient(api_key=api_key)

def linkup_query(query):
    response = client.search(
        query=query,
        depth="standard",
        output_type="sourcedAnswer",
        include_images=False,
    )
    return response.answer



def markdown_to_df(markdown_str):
    lines = markdown_str.strip().splitlines()
    content_lines = [line for line in lines if not line.strip().startswith("```")]
    content_lines = [line for i, line in enumerate(content_lines) if i != 1]
    csv_str = "\n".join(line.strip("|").strip() for line in content_lines)
    df = pd.read_csv(StringIO(csv_str), sep="|")
    df.columns = df.columns.str.strip()
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
    return df

# use case example
#query = f"Popmart is a brand that has been trending. \n Is Popmart owned by a public company? Include any parent companies, conglomorates or majority equity holders. If it is public, can you return the brand name, company name, ticker, exchange symbol and the country the exchange is in, in a markdown table? If it is private, can you return the same markdown table but with private in the ticker column? Please return only the markdown table and nothing else. The table columns should be, Brand, Company, Ticker, Exchange, Country."
#markdown_str = linkup_query(query)
#df = markdown_to_df(markdown_str)

