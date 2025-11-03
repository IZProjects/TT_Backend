from supabase_client import supabase
from utils.Apify_functions import tiktok_hashtag_analytics
from datetime import datetime
from zoneinfo import ZoneInfo
from utils.helpers import clean_table, chunk_list, trends_to_actual, split_last_pair




def tiktok_120day_trend():
    response = (
        supabase.table("tiktok2")
        .select("hashtag")
        .execute()
    )
    data = response.data
    chunks = chunk_list(data)

    for chunk in chunks:
        hashtags = [item["hashtag"] for item in chunk]
        apify_120days = tiktok_hashtag_analytics(hashtags, analytics_period="120")
        for i in range(len(apify_120days)):
            try:
                data_120days = apify_120days[i]
                trend_120days = data_120days['analytics']['trend']
                formatted_trend_120days = [(f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: "
                                            f"{item['value']}") for item in trend_120days]
                views_120days = data_120days['analytics']['video_views']
                trend_views_120days = trends_to_actual(formatted_trend_120days, int(views_120days))
                trend_string_120days = ', '.join(trend_views_120days)

                # ------------------------------------------- save to DB --------------------------------------------------
                response = (
                    supabase.table("tiktok2")
                    .upsert(
                        {
                            "hashtag": data_120days['analytics']['hashtag_name'],
                            "trend_120days": trend_string_120days,
                            "views_120days": views_120days,
                            "created_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat(),
                        },
                        on_conflict="hashtag",
                    )
                    .execute()
                )
            except Exception as e:
                print(f"❌ Skipping {apify_120days[i]['analytics']['hashtag_name']} due to error: {e}")
                continue



def tiktok_3yr_trend():
    response = (
        supabase.table("tiktok2")
        .select("hashtag")
        .execute()
    )
    data = response.data
    chunks = chunk_list(data)

    # ------------------------------------------- get data ------------------------------------------------------------
    for chunk in chunks:
        apify = tiktok_hashtag_analytics(chunk)
        for i in range(len(apify)):
            try:
                # ------------------------------------------- parse appify 3 yr data ----------------------------------
                data = apify[i]
                hashtag_name = data['hashtag_name']
                trend = data['analytics']['trend']
                formatted_trend = [f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: {item['value']}"
                                   for item in trend]

                ages = data['analytics']['audience_ages_readable']
                formatted_ages = [f"{item['age_range']}: {item['score']}" for item in ages]
                ages_string = ', '.join(formatted_ages)

                views = str(data['analytics']['video_views'])
                posts = str(data['analytics']['publish_cnt'])
                views_all = str(data['analytics']['video_views_all'])
                posts_all = str(data['analytics']['publish_cnt_all'])

                trend_views = trends_to_actual(formatted_trend, int(views))
                trend_string = ', '.join(trend_views)
                past_trend, proj_trend = split_last_pair(trend_string)

                countries = data['analytics']['audience_countries']
                formatted_countries = [f"{item['country_info']['value']}: {item['score']}" for item in countries]
                countries_string = ', '.join(formatted_countries)

                category = data['analytics']['industry_info']['value']

                related_hashtags = data['analytics']['related_hashtags']
                formatted_hashtags = [item['hashtag_name'] for item in related_hashtags]
                hashtags_string = ', '.join(formatted_hashtags)

                # ------------------------------------------- save to DB --------------------------------------------------
                response = (
                    supabase.table("tiktok2")
                    .upsert(
                        {
                            "hashtag": hashtag_name,
                            "trend": past_trend,
                            "ages": ages_string,
                            "views_3y": views,
                            "posts_3y": posts,
                            "views_all": views_all,
                            "posts_all": posts_all,
                            "countries": countries_string,
                            "categories": category,
                            "related_hashtag": hashtags_string,
                            "trend_projected": proj_trend,
                            "created_at": datetime.now(ZoneInfo("Australia/Sydney")).isoformat(),
                        },
                        on_conflict="hashtag",
                    )
                    .execute()
                )
            except Exception as e:
                print(f"❌ Skipping keyword '{apify[i]['hashtag_name']}' due to error: {e}")
                continue


tiktok_3yr_trend()
tiktok_120day_trend()
clean_table("tiktok2")