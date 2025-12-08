from supabase_client import supabase
from utils.helpers import last_value_and_yoy

kw_joined = supabase.table("kw_joined").select("*").execute().data

tiktok2 = supabase.table("tiktok2").select("*").execute().data

for d in tiktok2:
  value = d['trend'] + ', ' + d['trend_projected']
  date_str, volume, yoy = last_value_and_yoy(value, backtrack_size=2)

  other_data = {
      'ages': d['ages'],
      'views_3y': d['views_3y'],
      'posts_3y': d['posts_3y'],
      'views_all': d['views_all'],
      'views_120days': d['views_120days'],
      'posts_all': d['posts_all'],
      'countries': d['countries'],
      'related_hashtag': d['related_hashtag'],
  }

  if yoy is not None:
    yoy = int(yoy)

  response = (
              supabase.table("kw_joined")
              .upsert(
                  {
                      "keyword": d['hashtag'],
                      "updated_at": d['updated_at'],
                      "trend": d['trend'],
                      "categories": d['trend'],
                      "type": 'Tiktok',
                      "volume": volume,
                      "yoy": yoy,
                      "trend_projected": d['trend_projected'],
                      "categories": d['categories'],
                      "trend_st": d['trend_120days'],
                      "description": d['description'],
                      "tickers": d['stocks'],
                      "other_data": other_data,
                      "impact_score": d['impact_score'],
                      "impact_counts": d['impact_counts']
                  },
                  on_conflict="keyword",
              ).execute()
            )
