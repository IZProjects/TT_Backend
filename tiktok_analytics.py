from supabase_client import supabase
from utils.Apify_functions import tiktok_hashtag_analytics
from datetime import datetime

# ------------------------------------------- functions ---------------------------------------------------------------
def chunk_list(lst, chunk_size=1000):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

# ------------------------------------------- get hastags -------------------------------------------------------------
response = (
    supabase.table("q4")
    .select("hashtag")
    .execute()
)
hashtags = [item["hashtag"] for item in response.data]
hashtags = [tag.strip().removeprefix('#') for group in hashtags for tag in group.split(',')]
hashtags = list(set(hashtags))

chunks = chunk_list(hashtags)

# ------------------------------------------- get data ----------------------------------------------------------------
for chunk in chunks:
    apify = tiktok_hashtag_analytics(chunk)
    for i in range(len(apify)):
        data = apify[i]
        hashtag_name = data['hashtag_name']
        trend = data['analytics']['trend']
        formatted_trend = [f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: {item['value']}" for item
                           in trend]
        trend_string = ', '.join(formatted_trend)

        ages = data['analytics']['audience_ages_readable']
        formatted_ages = [f"{item['age_range']}: {item['score']}" for item in ages]
        ages_string = ', '.join(formatted_ages)

        views = str(data['analytics']['video_views'])
        posts = str(data['analytics']['publish_cnt'])
        views_all = str(data['analytics']['video_views_all'])
        posts_all = str(data['analytics']['publish_cnt_all'])

        countries = data['analytics']['audience_countries']
        formatted_countries = [f"{item['country_info']['value']}: {item['score']}" for item in countries]
        countries_string = ', '.join(formatted_countries)

        category = data['analytics']['industry_info']['value']

        related_hashtags = data['analytics']['related_hashtags']
        formatted_hashtags = [item['hashtag_name'] for item in related_hashtags]
        hashtags_string = ', '.join(formatted_hashtags)

        # ------------------------------------------- save to DB ------------------------------------------------------
        response = (
            supabase.table("tiktok_analytics")
            .upsert(
                {
                    "hashtag": hashtag_name,
                    "trend": trend_string,
                    "ages": ages_string,
                    "views_3y": views,
                    "posts_3y": posts,
                    "views_all": views_all,
                    "posts_all": posts_all,
                    "countries": countries_string,
                    "categories": category,
                    "related_hashtag": hashtags_string,
                },
                on_conflict="hashtag",
            )
            .execute()
        )