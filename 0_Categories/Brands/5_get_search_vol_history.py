from utils.DataforSEO_functions import get_trend, get_monthly_vol
import pandas as pd
import sqlite3

conn = sqlite3.connect('../../data.db')
df_companies = pd.read_sql('SELECT * FROM Brand_Stock_Price', conn)
df_companies = df_companies.dropna()
df_companies = df_companies.reset_index(drop=True)
df_companies['Search Volume'] = None

df_searchVol = pd.read_sql('SELECT * FROM Brand_search_vols', conn)


for i in range(len(df_companies)):
    try:
        keyword = df_companies.at[i,'Keyword']
        trend = get_trend(keyword,"past_5_years")
        known_volume = df_searchVol[df_searchVol['keyword'].str.lower() == keyword.lower().strip()]['search_volume'].iloc[0]
        df_companies.at[i,'Search Volume'] = get_monthly_vol(trend, known_volume)
    except Exception as e:
        print(f"An error has occurred for {keyword}: {e}")

df_companies = df_companies.dropna()
df_companies = df_companies.reset_index(drop=True)

df_companies.to_sql('Brand_Stock_Price_SVol', conn, if_exists='replace', index=False)
df_companies.to_csv(r"../../Brand_Stock_Price_SVol.csv")

