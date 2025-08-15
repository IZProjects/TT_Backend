import pandas as pd
from supabase_client import supabase
from utils.DataforSEO_functions import get_volume

# ------------------------------------------- functions ---------------------------------------------------------------
def chunk_list(lst, chunk_size=1000):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


# ---------------------------------------- get keywords chunks --------------------------------------------------------
response = (
    supabase.table("q1")
    .select("keyword")
    .execute()
)
keywords = [item["keyword"] for item in response.data]

keywords_chk = chunk_list(keywords)
# ---------------------------------------- get search vol -------------------------------------------------------------
for i in range(len(keywords_chk)):
    vols = get_volume(keywords_chk[i])
    for d in vols:
        key, value = next(iter(d.items()))
        # ----------------------------------------- save to DB --------------------------------------------------------
        response = (
            supabase.table("kw_search_vol")
            .upsert(
                {"keyword": key, "search_volume": value},
                on_conflict="keyword",
            )
            .execute()
        )




