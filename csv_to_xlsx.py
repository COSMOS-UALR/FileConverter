from tqdm import tqdm
import pandas as pd 
import json
import os


def save_excel(data, fname, loc="export"):
    df = pd.DataFrame(data)
    path = os.path.join(os.getcwd(), loc+"\\"+fname+".xlsx")
    #Removing Timezones
    # df['published_date'].apply(lambda x:datetime.datetime.replace(x,tzinfo=None))
    with pd.ExcelWriter(path, engine='xlsxwriter', options={'strings_to_urls': False}) as writer: 
        df.to_excel(writer, header=True, index=False, encoding='utf-8', na_rep='None')
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
