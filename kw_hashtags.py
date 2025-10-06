from supabase_client import supabase
import os


def run_kw_hashtags_script():
    # --- 1. Fetch q4 and tiktok_analytics ---
    q4_data = supabase.table("q4").select("keyword, hashtag").execute().data
    tiktok_data = supabase.table("tiktok_analytics").select("hashtag").execute().data

    # Convert tiktok hashtags to lowercase set for fast lookup
    tiktok_hashtags = {row["hashtag"].lower() for row in tiktok_data}

    # --- 2. Build keyword-hashtag pairs ---
    insert_rows = []
    for row in q4_data:
        keyword = row["keyword"]
        hashtags_str = row["hashtag"] or ""

        hashtags_list = [
            tag.strip().lstrip("#").lower()
            for tag in hashtags_str.split(",")
            if tag.strip()
        ]

        for tag in hashtags_list:
            if tag in tiktok_hashtags:
                insert_rows.append({
                    "keyword": keyword,
                    "hashtag": tag
                })

    # --- 3. Remove duplicates in batch ---
    unique_rows = [dict(t) for t in {tuple(d.items()) for d in insert_rows}]

    # --- 4. Upsert into kw_hashtag ---
    if unique_rows:
        response = (
            supabase.table("kw_hashtags")
            .upsert(
                unique_rows,
                on_conflict="keyword, hashtag",
                ignore_duplicates=True,
            )
            .execute()
        )
    else:
        print("No matches found.")
