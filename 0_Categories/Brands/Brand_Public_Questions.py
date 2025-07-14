import pandas as pd
from utils.Linkup_functions import linkup_query, markdown_to_df
import sqlite3



conn = sqlite3.connect('../../data.db')

SQLquery = """
SELECT *
FROM lifestyle_5yr AS l
JOIN (
    SELECT * FROM Public_Brands
    WHERE LOWER(Ticker) != 'private'
) AS p
ON LOWER(l.Keyword) = LOWER(p.Brand)
"""

df = pd.read_sql_query(SQLquery, conn)


rows=[]
for i in range(len(df)):
    query = f"{df.at[i,'Keyword']} is a brand that has been trending. \n Here is a description of what they do: {df.at[i,'Description']}. \n Can you help me figure out why they are trending and what products or services has caused the brand to increase in popularity and what is propelling its growth?. Word your response not as an answer to a question but as an excerpt from an analyst report. Make your answer easy to read like adding dot points where needed. Also give the response in markdown format."
    markdown_str = linkup_query(query)
    rows.append([df.at[i,'Keyword'], 'Why is this brand trending', markdown_str])

for i in range(len(df)):
    query = f"The brand {df.at[i,'Keyword']} owned by {df.at[i,'Company']} ({df.at[i,'Company']}:{df.at[i,'Ticker']}) has been trending in popularity.Considering {df.at[i,'Company']}'s ({df.at[i,'Company']}:{df.at[i,'Ticker']}) revenue mix, would an increase in the {df.at[i,'Keyword']} brand's popularity significantly impact revenues for the company?Word your response not as an answer to a question but as an excerpt from an analyst report. Answer the question directly and give figures and numbers where possible. Also give the response in markdown format."
    markdown_str = linkup_query(query)
    rows.append([df.at[i,'Keyword'], 'Does it impact revenues', markdown_str])

for i in range(len(df)):
    query = f"{df.at[i,'Keyword']} is a brand that has been trending. \n Here is a description of what they do: {df.at[i,'Description']}. \n Do you think the brand popularity will still growth further, slow down but still grow, peak, or decline over the next year?. Word your response not as an answer to a question but as an excerpt from an analyst report. Give reasons in your answer and provide evidence. Also give the response in markdown format."
    markdown_str = linkup_query(query)
    rows.append([df.at[i,'Keyword'], 'What does the future trend look like', markdown_str])

for i in range(len(df)):
    query = f"{df.at[i,'Keyword']} is a brand that has been trending. \n Here is a description of what they do: {df.at[i,'Description']}. \n Can you give me what their most popular products and/or services in order? Give it to me in a python dictionary form like so name:type"
    markdown_str = linkup_query(query)
    rows.append([df.at[i,'Keyword'], 'What are the most popular poducts', markdown_str])

for i in range(len(df)):
    query = f"{df.at[i,'Keyword']} is a brand that has been trending. \n Can you give me some search-term keywords which are closely associated with Popmart. Give me the top 5 in the form of a python list?"
    markdown_str = linkup_query(query)
    rows.append([df.at[i,'Keyword'], 'What are some keywords', markdown_str])

df = pd.DataFrame(rows, columns=["Brand", "Question", "Answer"])
df.to_sql('Public_Brands_Descriptions', conn, if_exists='replace', index=False)
print(df)
conn.close()