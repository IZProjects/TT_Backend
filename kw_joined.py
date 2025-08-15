from supabase_client import supabase


# ------------------------------------------- get search vol & categories ---------------------------------------------
kw_svc = supabase.table("kw_search_vol").select("keyword, search_volume, kw_category!inner(categories)").execute().data
print(kw_svc)

# ------------------------------------------- get kw tickers ----------------------------------------------------------
q3_data = supabase.table("q3").select("keyword, ticker").execute().data

grouped = {}
for item in q3_data:
    grouped.setdefault(item['keyword'], []).append(item['ticker'])

kw_tickers = [{'keyword': k, 'tickers': ', '.join(v)} for k, v in grouped.items()]
print(kw_tickers)

# ------------------------------------------- get kw hashtag ----------------------------------------------------------
kw_hashtags_data = supabase.table("kw_hashtags").select("keyword, hashtag").execute().data

grouped = {}
for item in kw_hashtags_data:
    grouped.setdefault(item['keyword'], []).append(item['hashtag'])

kw_hashtags = [{'keyword': k, 'hashtag': ', '.join(v)} for k, v in grouped.items()]
print(kw_hashtags)


# ------------------------------------------- get kw hashtag ----------------------------------------------------------
sv_dict = {d['keyword']: d['search_volume'] for d in kw_svc}
cat_dict = {d['keyword']: d['kw_category']['categories'] for d in kw_svc}
tickers_dict = {d['keyword']: d['tickers'] for d in kw_tickers}
hashtags_dict = {d['keyword']: d['hashtag'] for d in kw_hashtags}

# Merge only if keyword is in both list1 and list2
common_keywords = sv_dict.keys() & tickers_dict.keys()

merged = [{
    'keyword': k,
    'search_volume': sv_dict[k],
    'categories': cat_dict[k],
    'tickers': tickers_dict[k],
    'hashtag': hashtags_dict.get(k, "")  # empty string if not in list3
} for k in common_keywords]

BATCH = 500
for i in range(0, len(merged), BATCH):
    batch = merged[i:i+BATCH]
    (
        supabase
        .table("kw_joined")
        .upsert(batch, on_conflict="keyword")   # requires UNIQUE constraint on 'keyword'
        .execute()
    )