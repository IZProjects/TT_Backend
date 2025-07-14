import sqlite3
import pandas as pd



conn = sqlite3.connect('data.db')
df = pd.read_sql('SELECT * FROM Public_Brands', conn)
df = df.reset_index(drop=True)
df.to_csv(r"Public_Brands.csv")