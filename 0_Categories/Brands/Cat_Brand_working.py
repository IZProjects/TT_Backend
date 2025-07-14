import sqlite3
import pandas as pd
conn = sqlite3.connect('../../data.db')


df = pd.read_sql('SELECT * FROM Public_Brands', conn)
df.to_csv(r"C:\00 App Projects\TradingTrends\TT_Backend\Public.csv")

print(df)