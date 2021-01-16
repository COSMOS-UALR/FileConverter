from tqdm import tqdm
import pandas as pd 
import json
import os


def save_excel(data, fname, loc="export"):
    df = pd.DataFrame(data)
    path = os.path.join(os.getcwd(), loc+"\\"+fname+".csv")
    #Removing Timezones
    # df['published_date'].apply(lambda x:datetime.datetime.replace(x,tzinfo=None))
    df.to_csv(path, header=True, mode='w', index=False, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    del df
    return fname


def main():
    for _, _, fnames in os.walk("json_files"):
        with tqdm(total=len([x for x in fnames if '.json' in x])) as pbar:
            for fname in fnames:
                pbar.set_description(fname.replace('.json',''))
                if '.json' in fname:
                    with open(f"json_files//{fname}", encoding='utf-8') as f:
                        data = json.load(f, strict=False)
                        save_excel(data, fname.replace('.json',''))
                        pbar.update(1)

    print("\n\nAll Files finished! ")




if __name__ == "__main__":
    main()
