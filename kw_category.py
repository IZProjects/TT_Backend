import os
from dotenv import load_dotenv
from openai import OpenAI
from supabase_client import supabase

# ------------------------------------------- set up OpenAI -----------------------------------------------------------
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()

# ------------------------------------------- OpenAI Query Function ---------------------------------------------------
def ask_gpt(query):
    response = client.responses.create(
        model="gpt-5-nano",
        input=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You will be provided with some a topic, a description of why the topic is popular "
                                "and a list of category names. "
                                "Your job is to determine what are the best categories that represent the topic. "
                                "You must choose 1 but are allowed to choose up to 3. "
                                "Response with only the category name. "
                                "Seperate the categories with a comma if there are multiple. "
                                "Do not add any other punctuations and keep everything lowercase."
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": query
                    }
                ]
            }
        ],
        #temperature=0,
    )
    result = response.output[1].content[0].text
    return(result)

# ------------------------------------------- Other Function ----------------------------------------------------------
def capitalize(text):
    categories = text.split(",")
    categories = [cat.strip().title() for cat in categories]
    return ", ".join(categories)


def run_kw_category_script():
    # ---------------------------------------- get keywords ---------------------------------------------------------------
    response = supabase.rpc("get_new_data_kw_cat").execute()
    data = response.data

    # ---------------------------------------- Run GTP quer ---------------------------------------------------------------
    categories_str = "apparel & accessories, baby, kids & maternity, beauty & personal care, business services, education, financial services, food & beverage, games, health, home improvement, household products, life services, news & entertainment, pets, sports & outdoor, tech & electronics"

    for row in data:
        try:
            keyword = row['keyword']
            description = row['description']
            query = (f"Topic: {keyword}. Description: {description}. Categories: {categories_str}. "
                     f"What are the best categories would best suit the topic?")
            answer = ask_gpt(query)
            answer = capitalize(answer)
            # ----------------------------------------- save to DB --------------------------------------------------------
            response = (
                supabase.table("kw_category")
                .upsert(
                    {"keyword": keyword, "categories": answer},
                    on_conflict="keyword",
                )
                .execute()
            )
        except Exception as e:
            print(f"❌ Skipping keyword '{row['keyword']}' due to error: {e}")
            continue
