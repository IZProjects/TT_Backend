from supabase_client import supabase
import pandas as pd

"""response = (
    supabase.table("tiktok_analytics")
    .select("*")
    .range(3, 100)
    .execute()
)
data = response.data
print(len(data))"""

check = (
    supabase.table("kw_joined")
    .select("description")
    .eq("keyword", "dsfsdfsd")
    .execute()
)
if not check.data:
    print(1)
elif check.data[0]['description'] == None:
    print(2)
elif check.data[0]['description'] == "":
    print(3)



