from utils.DataforSEO_functions import get_volume
import pandas as pd
import sqlite3
import json

today = pd.Timestamp.today()
last_completed_month = (pd.Timestamp(today.year, today.month, 1) - pd.DateOffset(months=1)).strftime("%m/%d/%Y")

conn = sqlite3.connect('../../data.db')
df_companies = pd.read_sql('SELECT * FROM Brand_Stock_Price', conn)
keywords = df_companies['Keyword'].to_list()
vols = get_volume(keywords)
df = pd.DataFrame(vols)
df['Month'] = last_completed_month
df['country_distribution'] = df['country_distribution'].apply(json.dumps)
df.to_sql('Brand_search_vols', conn, if_exists='replace', index=False)
df.to_csv(r"../../brand_search_vol.csv")