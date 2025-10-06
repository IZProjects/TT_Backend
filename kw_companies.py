from supabase_client import supabase
from collections import defaultdict
import json
from utils.Linkup_functions import linkup_query

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def create_kw_companies():
    response = supabase.table("kw_joined").delete().is_("tickers", None).execute()
    kw_joined = supabase.table("kw_joined").select("keyword, type, trend, trend_projected, tickers").execute().data

    # reorganise by ticker
    grouped = defaultdict(lambda: {"keywords": [], "exchange": None, "full_name": None})

    for row in kw_joined:
        for ticker_info in row["tickers"]:
            ticker = ticker_info["ticker"]

            # initialise exchange/full_name once (safe if consistent per ticker)
            grouped[ticker]["exchange"] = ticker_info["exchange"]
            grouped[ticker]["full_name"] = ticker_info["full_name"]

            # add keyword with metadata
            grouped[ticker]["keywords"].append({
                "keyword": row["keyword"],
                "type": row["type"],
                "trend": row["trend"],
                "trend_projected": row["trend_projected"]
            })

    # final list of dicts for insertion
    ticker_table = [
        {
            "ticker": ticker,
            "exchange": info["exchange"],
            "full_name": info["full_name"],
            "keywords": info["keywords"]
        }
        for ticker, info in grouped.items()
        if ticker is not None
    ]

    tickers = [row["ticker"] for row in ticker_table]

    kw_ticker = (
        supabase.table("kw_tickers")
        .select("ticker, code, source")
        .in_("ticker", tickers)
        .execute()
        .data
    )

    kw_lookup = {row["ticker"]: row for row in kw_ticker}

    for t in ticker_table:
        ticker = t["ticker"]
        if ticker in kw_lookup:
            match = kw_lookup[ticker]
            t["code"] = match["code"]
            t["source"] = match["source"]
            t['ticker_id'] = f"{ticker}.{t['code']}"
            check = (
                supabase.table("kw_companies")
                .select("description")
                .eq("ticker", t['ticker'])
                .execute()
            )
            if not check.data:
                t['description'] = linkup_query(f"Can you give me a description of what {t['full_name']} ({t['ticker']}) does?")
            elif check.data[0]['description'] == None:
                t['description'] = linkup_query(f"Can you give me a description of what {t['full_name']} ({t['ticker']}) does?")
            elif check.data[0]['description'] == "":
                t['description'] = linkup_query(f"Can you give me a description of what {t['full_name']} ({t['ticker']}) does?")
            else:
                pass

    print(ticker_table)
    for batch in chunked(ticker_table, 500):
        # supabase-py will JSON-encode lists/dicts automatically
        # on_conflict ensures 'ticker' is treated as the unique key for upsert behavior
        supabase.table("kw_companies").upsert(batch, on_conflict="ticker_id").execute()

