import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd
from openai import OpenAI
from utils.Linkup_functions import linkup_query

#------------- load variables --------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

#------------- load ET data ----------------------------------------
df = pd.read_csv(r"../../data/lifestyle_5yr.csv")
df['Type'] = 'Unclassified'
df = df[(df['Volume'] > 1000)]
df = df.reset_index(drop=True)


#-------------- run AI model -----------------------------------------
client = OpenAI()
for i in range(len(df)):
  response = client.responses.create(
    model="gpt-4o-mini",
    input=[
      {
        "role": "system",
        "content": [
          {
            "type": "input_text",
            "text": "You will be provided with a text, and your task is to determine whether it is referring a product, brand or theme? Answer with only 1 word of these words. Do not add any punctuations and capitalise the first letter."
          }
        ]
      },
      {
        "role": "user",
        "content": [
          {
            "type": "input_text",
            "text": df.at[i,'Description']
          }
        ]
      }
    ],
    temperature=0,
  )
  result = response.output[0].content[0].text
  print(result)
  df.at[i, 'Type'] = result
  #print(f"{df.at[i,'Keyword']}: {result}")

"""for i in range(len(df)):
  query = f"Is {df.at[i,'Description']} referring to a product, brand or theme? Answer with only 1 word, don't add any periods and capitalise the first letter."
  result = linkup_query(query)
  print(f"{df.at[i,'Keyword']}: {result}")"""

# ------------------ write to db -----------------------------
conn = sqlite3.connect('../../data.db')
df.to_sql('lifestyle_5yr', conn, if_exists='replace', index=False)
conn.close()
df.to_csv(r"../../initial_data.csv")