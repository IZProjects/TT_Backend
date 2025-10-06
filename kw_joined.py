from supabase_client import supabase
from datetime import datetime
from utils.Linkup_functions import linkup_query

def last_value_and_yoy(trend_string: str, backtrack_size=2):
    """
    :param trend_string: "06/01/2024: 10783, 07/01/2024: 77881, 08/01/2024: 94655, ..."
    :return: (second_last_month_str, value_of_second_last_month, YoY_percent)
             - month string in MM/DD/YYYY
             - value (int)
             - YoY as % (float) or None if unavailable / prev is 0
    """
    # Parse "MM/DD/YYYY: value" pairs
    parts = [p.strip() for p in trend_string.strip().strip(",").split(",") if p.strip()]
    data = {}
    for p in parts:
        date_str, val_str = [x.strip() for x in p.split(":", 1)]
        dt = datetime.strptime(date_str, "%m/%d/%Y").date()
        val = int(val_str.replace(",", ""))
        data[dt] = val

    # Need at least two months to get the second-last
    if len(data) < backtrack_size:
        return None, None, None

    # Second-last month by date
    dates = sorted(data)
    target_dt = dates[-backtrack_size]
    target_val = data[target_dt]

    # YoY for that month
    yoy_dt = target_dt.replace(year=target_dt.year - 1)
    prev_val = data.get(yoy_dt)
    if prev_val in (None, 0):
        yoy = None
    else:
        yoy = (target_val - prev_val) / prev_val * 100.0

    return target_dt.strftime("%m/%d/%Y"), target_val, yoy


def create_kw_joined():
    # ------------------------------------------- get search vol & categories -----------------------------------------
    kw_svc = (supabase.table("kw_search_vol").select("keyword, search_volume, search_volume_projected, "
                                                    "search_volume_90days", "kw_category!inner(categories)")
              .execute().data)
    kw_svc = [{**row, 'type': 'Google Search'} for row in kw_svc]

    # ------------------------------------------- get kw tickers ------------------------------------------------------
    # Fetch data from Supabase
    q3_data = supabase.table("q3").select("keyword, ticker, full_name, exchange").execute().data
    kw_tickers_data = supabase.table("kw_tickers").select("original_ticker, ticker, code").execute().data

    # Build a lookup dictionary for faster matching
    kw_lookup = {row['original_ticker']: row for row in kw_tickers_data}

    # Group by keyword, only including tickers that exist in kw_tickers_data
    grouped = {}
    for item in q3_data:
        match = kw_lookup.get(item['ticker'])
        if match:  # only add if ticker exists in kw_tickers_data
            grouped.setdefault(item['keyword'], []).append({
                'ticker': match['ticker'],
                'full_name': item['full_name'],
                'exchange': item['exchange'],
                'code': match['code']
            })

    # Convert grouped dict into final list of dicts
    kw_tickers = [{'keyword': k, 'tickers': v} for k, v in grouped.items()]

    # ------------------------------------------- get description -----------------------------------------------------
    q1_data = supabase.table("q1").select("keyword, description").execute().data

    # ------------------------------------------- prep kw data -------------------------------------------------------
    sv_dict = {d['keyword']: d['search_volume'] for d in kw_svc}
    sv_prj_dict = {d['keyword']: d['search_volume_projected'] for d in kw_svc}
    sv_90d_dict = {d['keyword']: d['search_volume_90days'] for d in kw_svc}
    cat_dict = {d['keyword']: d['kw_category']['categories'] for d in kw_svc}
    type_dict = {d['keyword']: d['type'] for d in kw_svc}
    tickers_dict = {d['keyword']: d['tickers'] for d in kw_tickers}
    desc_dict = {d['keyword']: d['description'] for d in q1_data}

    # ------------------------------------------- get last month volume & yoy -----------------------------------------
    volume_dict = {}
    yoy_dict = {}
    for key, value in sv_dict.items():
        try:
            prj_value = sv_prj_dict.get(key)
            trend_total = value + ', ' + prj_value
            date_str, volume, yoy = last_value_and_yoy(trend_total, backtrack_size=3)
            volume_dict[key] = volume
            yoy_dict[key] = int(yoy) if yoy is not None else None
        except Exception as e:
            print(f"❌ Skipping '{key}' due to error: {e}")
            continue

    # ------------------------------------------- merge kw data -------------------------------------------------------
    common_keywords = sv_dict.keys() & tickers_dict.keys()
    merged = []
    for k in common_keywords:
        try:
            merged.append({
                'keyword': k,
                'trend': sv_dict[k],
                'categories': cat_dict[k],
                'tickers': tickers_dict[k],
                'type': type_dict[k],
                'volume': volume_dict[k],
                'yoy': yoy_dict[k],
                'trend_projected': sv_prj_dict[k],
                'trend_st': sv_90d_dict[k],
                'description': desc_dict[k]
            })
        except KeyError as e:
            # Skip this keyword if any dict is missing it
            print(f"Skipping {k}, missing key: {e}")
        except Exception as e:
            # Catch-all for any other unexpected error
            print(f"Error processing {k}: {e}")


    # ------------------------------------------- get hashtag data ----------------------------------------------------
    kw_hashtags_data = supabase.table("kw_hashtags").select("keyword, hashtag").execute().data

    grouped = {}
    for item in kw_hashtags_data:
        grouped.setdefault(item['keyword'], []).append(item['hashtag'])

    kw_hashtags = [{'keyword': k, 'hashtag': ', '.join(v)} for k, v in grouped.items()]

    hashtags_data = (supabase.table("tiktok_analytics").select("hashtag, trend, categories, trend_projected, "
                                                               "trend_120days")
                     .execute().data)
    hashtag_to_keyword = {}
    for item in kw_hashtags:
        hashtags = [h.strip() for h in item['hashtag'].split(',')]
        for h in hashtags:
            hashtag_to_keyword[h] = item['keyword']

    # Add keyword to each dict in hashtags_data if it matches
    for item in hashtags_data:
        if item['hashtag'] in hashtag_to_keyword:
            item['keyword'] = hashtag_to_keyword[item['hashtag']]

    # Build a mapping of keyword → tickers from kw_tickers
    keyword_to_tickers = {item['keyword']: item['tickers'] for item in kw_tickers}

    # Add tickers to hashtags_data if keyword exists
    for item in hashtags_data:
        if 'keyword' in item and item['keyword'] in keyword_to_tickers:
            item['tickers'] = keyword_to_tickers[item['keyword']]

    hashtags_data = [{**row, 'type': 'Tiktok'} for row in hashtags_data]

    for item in hashtags_data:
        item.pop('keyword', None)  # safely remove if it exists

    for item in hashtags_data:
        item['keyword'] = item.pop('hashtag')

    for item in hashtags_data:
        item['trend_st'] = item.pop('trend_120days')


    ordered_keys = ['keyword', 'trend', 'categories', 'tickers', 'type', 'trend_projected', 'trend_st']

    hashtags_data = [
        {key: item.get(key) for key in ordered_keys}
        for item in hashtags_data
    ]


    for d in hashtags_data:
        value = d['trend'] + ', ' + d['trend_projected']
        date_str, volume, yoy = last_value_and_yoy(value, backtrack_size=2)
        d['volume'] = volume
        d['yoy'] = int(yoy)
        check = (
            supabase.table("kw_joined")
            .select("description")
            .eq("keyword", d['keyword'])
            .execute()
        )
        if not check.data:
            d['description'] = linkup_query(f"#{d['keyword']} has been trending on Tiktok. "
                                            f"Can you tell me why it is trending? Provide your answer in markdown format "
                                            f"starting with a small title.")
        elif check.data[0]['description'] == None:
            d['description'] = linkup_query(f"#{d['keyword']} has been trending on Tiktok. "
                                            f"Can you tell me why it is trending? Provide your answer in markdown format "
                                            f"starting with a small title.")
        elif check.data[0]['description'] == "":
            d['description'] = linkup_query(f"#{d['keyword']} has been trending on Tiktok. "
                                            f"Can you tell me why it is trending? Provide your answer in markdown format "
                                            f"starting with a small title.")
        else:
            pass

    joined = merged + hashtags_data

    BATCH = 500
    for i in range(0, len(joined), BATCH):
        batch = joined[i:i+BATCH]
        (
            supabase
            .table("kw_joined")
            .upsert(batch, on_conflict="keyword")   # requires UNIQUE constraint on 'keyword'
            .execute()
        )

