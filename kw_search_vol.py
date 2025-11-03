import pandas as pd
from supabase_client import supabase
from utils.DataforSEO_functions import (get_volume, get_trend, gtrend_to_volumes, format_daily_volumes_str,
                                        monthly_totals_str)
import re
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.helpers import clean_table

# ------------------------------------------- functions ---------------------------------------------------------------
def chunk_list(lst, chunk_size=1000):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def remove_existing_dates(string1: str, string2: str) -> str:
    dates1 = set(re.findall(r'\b\d{2}/\d{2}/\d{4}\b', string1))
    pairs2 = re.findall(r'(\d{2}/\d{2}/\d{4})\s*:\s*([^,]+)', string2)
    filtered = [f"{d}: {v.strip()}" for d, v in pairs2 if d not in dates1]
    return ", ".join(filtered)

def monthly_search_vol_update():
    # ---------------------------------------- get keywords chunks ----------------------------------------------------
    response = (
        supabase.table("q1")
        .select("keyword")
        .execute()
    )
    keywords = [item["keyword"] for item in response.data]

    keywords_chk = chunk_list(keywords)
    # ---------------------------------------- get search vol ---------------------------------------------------------
    for i in range(len(keywords_chk)):
        vols = get_volume(keywords_chk[i])
        for d in vols:
            try:
                keyword, trend = next(iter(d.items()))
                # ----------------------------------------- save to DB ----------------------------------------------------
                response = (
                    supabase.table("kw_search_vol")
                    .upsert(
                        {
                            "keyword": keyword,
                            "search_volume": trend,
                            "created_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat(),
                        },
                        on_conflict="keyword",
                    )
                    .execute()
                )
            except Exception as e:
                keyword, trend = next(iter(d.items()))
                print(f"❌ Skipping {keyword} due to error: {e}")
                continue

    clean_table("kw_search_vol")



def weekly_search_vol_update():
    response = (
        supabase.table("kw_search_vol")
        .select("*")
        .execute()
    )

    data = response.data

    for i in range(len(data)):
        try:
            keyword = data[i]['keyword']
            #print(keyword)
            trend = data[i]['search_volume']
            known_month = trend.rstrip(", \n\t").rsplit(",", 1)[-1].strip()
            last90daystrend = get_trend(keyword, "past_90_days")
            daily_vols = gtrend_to_volumes(last90daystrend, known_month)
            daily_trend_str = format_daily_volumes_str(daily_vols)
            monthly_total = monthly_totals_str(daily_trend_str)
            projected_monthly_trend = remove_existing_dates(trend, monthly_total)
            #projected_monthly_trend = known_month + ', ' + projected_monthly_trend
            #print(projected_monthly_trend)
        # ----------------------------------------- save to DB ----------------------------------------------------
            response = (
                supabase.table("kw_search_vol")
                .upsert(
                    {
                        "keyword": keyword,
                        "created_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat(),
                        "search_volume": trend,
                        "search_volume_projected": projected_monthly_trend,
                        "search_volume_90days": daily_trend_str
                    },
                    on_conflict="keyword",
                )
                .execute()
            )
        except Exception as e:
            print(f"❌ Skipping keyword '{data[i]['keyword']}' due to error: {e}")
            continue

    clean_table("kw_search_vol")


#monthly_search_vol_update()
#weekly_search_vol_update()