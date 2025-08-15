from supabase_client import supabase
import pandas as pd

response = (
    supabase.table("q4")
    .select("*")
    .execute()
)
data = response.data
print(data)

response = (
    supabase.table("tiktok_analytics")
    .select("*")
    .execute()
)
data = response.data
print(data)



