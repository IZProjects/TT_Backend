import pandas as pd
from utils.Linkup_functions import linkup_query, markdown_to_df
import sqlite3


# ------------------ load data ------------------------------------

conn = sqlite3.connect('../../data.db')
df = pd.read_sql('SELECT * FROM lifestyle_5yr', conn)
df = df[
    (df['Brand'] == 'Yes') &
    (df['Type'].str.lower().isin(['brand', 'product']))
]
df = df.reset_index(drop=True)


# ------------------ Linkup AI Search -----------------------------
dfs=[]
for i in range(len(df)):
    #query = f"{df.at[i,'Keyword']} is a brand that has been trending. \n Is {df.at[i,'Keyword']} owned by a public company? Include any parent companies, conglomorates or majority equity holders. If it is public, can you return the keyword, company name, ticker, exchange symbol and the country the exchange is in, in a markdown table? If it is private, can you return the same markdown table but with private in the ticker column? Please return only the markdown table and nothing else. The table columns should be, Brand, Company, Ticker, Exchange, Country."
    #query = f"{df.at[i,'Keyword']} is a keyword that has been trending. \n Is {df.at[i,'Keyword']} strongly affiliated to a public company? Include any parent companies, conglomorates or majority equity holders. If it is affiliated to a public company, can you return the keyword, company name, ticker, exchange symbol and the country the exchange is in, in a markdown table? If it is private, can you return the same markdown table but with private in the ticker column? Please return only the markdown table and nothing else. The table columns should be, Brand, Company, Ticker, Exchange, Country."
    query = f"The keyword '{df.at[i,'Keyword']}' is a keyword referring to a product or a brand. Is the product/brand '{df.at[i,'Keyword']}' strongly affiliated to a public company? Include any parent companies, conglomorates or majority equity holders. If it is affiliated to a public company, can you return the keyword, company name, yfinance ticker, exchange and country in a markdown table? If it is not strongly affiliated to a public company please return 'N/A' in the markdown table? Please return only the markdown table and nothing else. The table columns should be, Brand, Company, Ticker, Exchange, Country."
    markdown_str = linkup_query(query)
    df_extracted = markdown_to_df(markdown_str)
    df_extracted.insert(0, 'Keyword', df.at[i,'Keyword'])
    print(df_extracted)
    dfs.append(df_extracted)

df_concat = pd.concat(dfs, ignore_index=True)
# ------------------ Create and write db -----------------------------
conn = sqlite3.connect('../../data.db')
df_concat.to_sql('Public_Brands', conn, if_exists='replace', index=False)
conn.close()

