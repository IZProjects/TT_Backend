from get_kw_list import run_kw_list_script
from the_linkups import run_linkup_sript
from kw_tickers import run_kw_tickers_script
from kw_stock_price import run_kw_stock_price_script
from kw_search_vol import weekly_search_vol_update, monthly_search_vol_update
from tiktok_analytics import weekly_tiktok_update, monthly_tiktok_update
from kw_hashtags import run_kw_hashtags_script
from kw_category import run_kw_category_script
from kw_joined import create_kw_joined
from kw_companies import create_kw_companies

print("-------------------------------- GETTING KW LIST --------------------------------")
kw_list = run_kw_list_script()
print("-------------------------------- RUNNING LINKUPS --------------------------------")
run_linkup_sript(kw_list)
print("-------------------------------- RUNNING kw_tickers --------------------------------")
run_kw_tickers_script()
print("-------------------------------- RUNNING kw_stock price --------------------------------")
run_kw_stock_price_script()
print("-------------------------------- RUNNING kw_category --------------------------------")
run_kw_category_script()
print("-------------------------------- UPDATING MONTHLY SEARCH VOL DATA --------------------------------")
monthly_search_vol_update()
print("-------------------------------- UPDATING WEEKLY SEARCH VOL DATA --------------------------------")
weekly_search_vol_update()
print("-------------------------------- UPDATING MONTHLY TIKTOK DATA --------------------------------")
monthly_tiktok_update()
print("-------------------------------- UPDATING WEEKLY TIKTOK DATA --------------------------------")
weekly_tiktok_update()
print("-------------------------------- RUNNING kw_hashtags --------------------------------")
run_kw_hashtags_script()
print("-------------------------------- CREATING kw_joined --------------------------------")
create_kw_joined()
print("-------------------------------- CREATING kw_companies --------------------------------")
create_kw_companies()
