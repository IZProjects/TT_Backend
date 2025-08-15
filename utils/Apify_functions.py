from apify_client import ApifyClient
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("APIFY_API_KEY")
client = ApifyClient(API_KEY)

# Prepare the Actor input


def tiktok_top100_with_analytics(analytics_period="1095", type="top100_with_analytics", new=False, period="30"):
    run_input = {
        "analytics_period": analytics_period,
        "result_type": type,
        "top100_new_on_board": new,
        "top100_period": period,  # chan be 7, 30, 120 for days
        # "top100_industry": "25000000000",
    }
    # Run the Actor and wait for it to finish
    run = client.actor("codebyte/tiktok-trending-hashtags-analytics").call(run_input=run_input)

    result = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        result.append(item)
        print(item)
    return result

def tiktok_hashtag_analytics(hashtag_list, analytics_period="1095", type="analytics", new=False):
    run_input = {
        "analytics_period": analytics_period,
        "hashtag_list": hashtag_list,
        "result_type": type,
        "top100_new_on_board": new
    }
    # Run the Actor and wait for it to finish
    run = client.actor("codebyte/tiktok-trending-hashtags-analytics").call(run_input=run_input)

    result = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        result.append(item)
    return result

"""from datetime import datetime

l = tiktok_hashtag_analytics(['popmart'])
data = l[0]
trend = data['analytics']['trend']
formatted_trend = [f"{datetime.utcfromtimestamp(item['time']).strftime('%m/%d/%Y')}: {item['value']}" for item in trend]
trend_string = ', '.join(formatted_trend)

ages = data['analytics']['audience_ages_readable']
formatted_ages = [f"{item['age_range']}: {item['score']}" for item in ages]
ages_string = ', '.join(formatted_ages)

views = data['analytics']['video_views']
posts = data['analytics']['publish_cnt']
views_all = data['analytics']['video_views_all']
posts_all = data['analytics']['publish_cnt_all']

countries = data['analytics']['audience_countries']
formatted_countries = [f"{item['country_info']['value']}: {item['score']}" for item in countries]
countries_string = ', '.join(formatted_countries)

category = data['analytics']['industry_info']['value']

related_hashtags = data['analytics']['related_hashtags']
formatted_hashtags = [item['hashtag_name'] for item in related_hashtags]
hashtags_string = ', '.join(formatted_hashtags)

print(f"trend: {trend_string}")
print(f"ages: {ages_string}")
print(f"views: {views}")
print(type(views))
print(f"posts: {posts}")
print(f"views_all: {views_all}")
print(f"posts_all: {posts_all}")
print(f"countries: {countries_string}")
print(f"category: {category}")
print(f"related_hashtags: {hashtags_string}")"""




