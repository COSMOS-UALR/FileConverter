from tqdm import tqdm
import pandas as pd 
import json
import os


def save_excel(df, fname, loc="export"):
    # df = pd.DataFrame(data)
    path = os.path.join(os.getcwd(), loc+"\\"+fname+".json")
    #Removing Timezones
    # df['published_date'].apply(lambda x:datetime.datetime.replace(x,tzinfo=None))
    df.to_json(path)
    del df
    return fname


def main():
    for _, _, fnames in os.walk("csv_files"):
        for fname in tqdm([x for x in fnames if '.csv' in x]):
            if '.csv' in fname:
                df = pd.read_csv(f"csv_files//{fname}")
                save_excel(df, fname.replace('.csv', ''))
                del df 




if __name__ == "__main__":
    main()
