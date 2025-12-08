from supabase_client import supabase
from collections import defaultdict
from utils.helpers import clean_table
from utils.OpenAI_functions import ask_gpt
from utils.DataforSEO_functions import get_SERP_AI

exchanges = ("NASDAQ Stock Market, New York Stock Exchange, OTC Markets Group, London Stock Exchange, "
             "Toronto Stock Exchange, Neo Exchange (Canada), TSX Venture Exchange, Börse Berlin, Börse Hamburg, "
             "Deutsche Börse Xetra, Börse Düsseldorf, Börse Hannover, Börse München (Munich), Börse Stuttgart, "
             "Börse Frankfurt, Luxembourg Stock Exchange, Wiener Börse (Vienna Stock Exchange), Euronext Paris, "
             "Euronext Brussels, Bolsas y Mercados Españoles (Spanish Exchanges), SIX Swiss Exchange, Euronext Lisbon, "
             "Euronext Amsterdam, Nasdaq Iceland, Euronext Dublin, Nasdaq Helsinki, Oslo Børs, Nasdaq Copenhagen, "
             "Nasdaq Stockholm, Victoria Falls Stock Exchange, Zimbabwe Stock Exchange, CBOE Europe (London), "
             "Uganda Securities Exchange, Börse Stuttgart Digital Exchange, Rwanda Stock Exchange, "
             "Prague Stock Exchange, Botswana Stock Exchange, Nigerian Stock Exchange, Nile Stock Exchange, "
             "Malawi Stock Exchange, Ghana Stock Exchange, Nairobi Securities Exchange, Casablanca Stock Exchange, "
             "Mauritius Stock Exchange, Tel Aviv Stock Exchange, Korea Exchange, KOSDAQ, Budapest Stock Exchange, "
             "Warsaw Stock Exchange, Philippine Stock Exchange, Shanghai Stock Exchange, Indonesia Stock Exchange, "
             "National Stock Exchange of India, Abu Dhabi Securities Exchange, Shenzhen Stock Exchange, "
             "Australian Securities Exchange, Santiago Stock Exchange, Johannesburg Stock Exchange, "
             "Pakistan Stock Exchange, Stock Exchange of Thailand, Colombian Stock Exchange, "
             "Ho Chi Minh Stock Exchange, Bursa Malaysia, Bucharest Stock Exchange, Buenos Aires Stock Exchange, "
             "B3 – Brasil Bolsa Balcão, Mexican Stock Exchange, Zagreb Stock Exchange, Taiwan Stock Exchange, "
             "Costa Rica Stock Exchange, Lima Stock Exchange")


S = """
You will be given either a stock exchange code (e.g., XNAS) or a stock exchange name.  
You will also be given a list containing valid full stock exchange names.  

Your task is to:
1. Identify the correct full stock exchange name that corresponds to the provided code or name.  
2. Return only the matching name exactly as it appears in the list.  

If no match exists, return "NA".
Do not provide explanations, alternatives, or additional text.
"""

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def create_kw_companies():
    kw_joined = (
        supabase.table("kw_joined")
        .select("*")
        .execute()
        .data
    )

    grouped = defaultdict(lambda: {
        "keywords": [],
        "exchange": None,
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
            grouped[ticker]["exchange"] = ask_gpt(query=f"exchange: {ti.get('exchange')}, list:{exchanges}",
                                                  system_prompt=S)
            grouped[ticker]["full_name"] = ti.get("full_name")
            grouped[ticker]["source"] = "EODHD"
            grouped[ticker]["code"] = ti.get("code")
            grouped[ticker]["keywords"].append({
                "keyword": row["keyword"],
                "type": row["type"],
                "trend": row["trend"],
                "trend_projected": row["trend_projected"],
                "relation": ti.get("relation"),
                "impact": ti.get("impact"),
                "direction": ti.get("direction"),
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
            "code": info["code"],
            "source": info["source"],
            "full_name": info["full_name"],
            "keywords": info["keywords"],
            "avg_yoy": _avg_int(info["yoy_sum"], info["yoy_count"]),
            "avg_volume": _avg_int(info["vol_sum"], info["vol_count"]),
        })


    enriched_rows = []
    for t in base_rows:
        if not t or not t.get("code"):  # no match => skip to avoid NULL ticker_id
            continue
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
            try:
                t["description"] = get_SERP_AI(f"Can you give me a description of what {t['full_name']} ({t['ticker']}) "
                                           f"does?")
            except Exception as e:
                print(f"get_SERP_AI failed {e}")
                t["description"] = None
        else:
            t["description"] = check.data[0].get("description")

        enriched_rows.append(t)

    # upsert only rows that have ticker_id
    for batch in chunked(enriched_rows, 500):
        supabase.table("kw_companies").upsert(batch, on_conflict="ticker_id").execute()

    #clean_table("kw_companies")



create_kw_companies()