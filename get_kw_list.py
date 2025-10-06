import pandas as pd
import glob
import os

def run_kw_list_script():
    # path to your folder with CSV files
    folder_path = "data"

    # collect filtered DataFrames
    dfs = []
    for file in glob.glob(os.path.join(folder_path, "*.csv")):
        df = pd.read_csv(file)

        # filter conditions
        df = df[
            (df['Growth Indicator'] != 'peaked') &
            (df['Speed Indicator'] != 'Stationary') &
            (df['Brand'] != 'Yes')
            ]

        dfs.append(df)

    # combine everything into one DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.head(500)

    # get 'Keyword' column as a list
    keywords = combined_df['Keyword'].tolist()
    keywords = list(set(keywords))
    return keywords
