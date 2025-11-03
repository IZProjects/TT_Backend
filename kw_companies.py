from supabase_client import supabase
from collections import defaultdict
import json
from utils.Linkup_functions import linkup_query
from utils.helpers import clean_table
from utils.OpenAI_functions import ask_gpt

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

countries = "USA, UK, Canada, Germany, Luxembourg, Austria, France, Belgium, Switzerland, Spain, Portugal, Netherlands, Finland, Iceland, Ireland, Norway, Denmark, Sweden, Zimbabwe, Zambia, Uganda, Tanzania, Rwanda, Czech Republic, Botswana, Nigeria, Egypt, Malawi, Ghana, Kenya, Morocco, Mauritius, Israel, Korea, Hungary, Poland, Philippines, Chile, Indonesia, China, India, Greece, Australia, South Africa, Pakistan, Thailand, Sri Lanka, Vietnam, Malaysia, Romania, Brazil, Argentina, Mexico, Croatia, Taiwan, Peru, Turkey, Unknown, Hong Kong, Japan, Saudi Arabia, Russia"

def create_kw_companies():
    system_prompt = ("You will be given a stock exchange name or code. Your job is return the country that stock "
                     f"exchange is in. You can choose fom this list: {countries}. Do not add any extra commentary, "
                     f"abbreviations or punctuation")

    kw_joined = (
        supabase.table("kw_joined")
        .select("keyword, type, trend, trend_projected, tickers, yoy, volume")
        .execute()
        .data
    )

    grouped = defaultdict(lambda: {
        "keywords": [],
        "exchange": None,
        "country": None,
        "full_name": None,
        "yoy_sum": 0, "yoy_count": 0,
        "vol_sum": 0, "vol_count": 0,
    })

    for row in kw_joined:
        yoy = row.get("yoy")
        vol = row.get("volume")
        for ti in row["tickers"]:
            ticker = ti.get("ticker")
            if ticker is None:
                continue
            grouped[ticker]["exchange"] = ti.get("exchange")
            grouped[ticker]["country"] = ask_gpt(ti.get("exchange"), system_prompt)
            grouped[ticker]["full_name"] = ti.get("full_name")
            grouped[ticker]["keywords"].append({
                "keyword": row["keyword"],
                "type": row["type"],
                "trend": row["trend"],
                "trend_projected": row["trend_projected"],
                "relation": ti.get("relation"),
            })
            if isinstance(yoy, (int, float)):
                grouped[ticker]["yoy_sum"] += yoy
                grouped[ticker]["yoy_count"] += 1
            if isinstance(vol, (int, float)):
                grouped[ticker]["vol_sum"] += vol
                grouped[ticker]["vol_count"] += 1

    def _avg_int(total, count):
        return int(round(total / count)) if count else None

    # base rows (no ticker_id yet)
    base_rows = []
    for ticker, info in grouped.items():
        if ticker is None:
            continue
        base_rows.append({
            "ticker": ticker,
            "exchange": info["exchange"],
            "country": info["country"],
            "full_name": info["full_name"],
            "keywords": info["keywords"],
            "avg_yoy": _avg_int(info["yoy_sum"], info["yoy_count"]),
            "avg_volume": _avg_int(info["vol_sum"], info["vol_count"]),
        })

    # enrich with kw_tickers (code/source). Only keep rows that have a match.
    tickers = [r["ticker"] for r in base_rows]
    kw_ticker = (
        supabase.table("kw_tickers")
        .select("ticker, code, source")
        .in_("ticker", tickers)
        .execute()
        .data
    )
    kw_lookup = {row["ticker"]: row for row in kw_ticker}

    enriched_rows = []
    for t in base_rows:
        m = kw_lookup.get(t["ticker"])
        if not m or not m.get("code"):   # no match => skip to avoid NULL ticker_id
            continue
        t["code"] = m["code"]
        t["source"] = m["source"]
        t["ticker_id"] = f"{t['ticker']}.{t['code']}"

        # fetch/generate description if needed
        check = (
            supabase.table("kw_companies")
            .select("description")
            .eq("ticker", t["ticker"])
            .execute()
        )
        need_desc = (not check.data) or (check.data[0].get("description") in (None, ""))
        if need_desc:
            t["description"] = linkup_query(
                f"Can you give me a description of what {t['full_name']} ({t['ticker']}) does?"
            )

        enriched_rows.append(t)

    # upsert only rows that have ticker_id
    for batch in chunked(enriched_rows, 500):
        supabase.table("kw_companies").upsert(batch, on_conflict="ticker_id").execute()

    clean_table("kw_companies")



create_kw_companies()