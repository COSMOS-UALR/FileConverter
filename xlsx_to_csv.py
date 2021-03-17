from tqdm import tqdm
import pandas as pd 
import json
import os


def save_excel(df, fname, loc="export"):
    # df = pd.DataFrame(data)
    path = os.path.join(os.getcwd(), loc+"\\"+fname+".csv")
    #Removing Timezones
    # df['published_date'].apply(lambda x:datetime.datetime.replace(x,tzinfo=None))
    df.to_csv(path, header=True, mode='w', index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    del df
    return fname


def main():
    for _, _, fnames in os.walk("xlsx_files"):
        for fname in tqdm([x for x in fnames if '.xlsx' in x]):
            if '.xlsx' in fname:
                df = pd.read_excel(f"xlsx_files//{fname}")
                save_excel(df, fname.replace('.xlsx', ''))
                del df 




if __name__ == "__main__":
    main()
