from tqdm import tqdm, trange
import json
import os



def main():
    for _, _, fnames in os.walk("json_files"):
        with tqdm(total=len([x for x in fnames if '.json' in x])) as pbar:
            for fname in fnames:
                pbar.set_description(fname.replace('.json',''))
                if '.json' in fname:
                    with open(f"json_files//{fname}", encoding='utf-8') as f:
                        data = json.load(f, strict=False)
                    #Converting to list of dicts
                    keys = list(data.keys())
                    num_values = len(data[keys[0]])
                    new_data = []
                    for i in trange(0, num_values, desc="Converting"):
                        d = {}
                        for key in keys:
                            d[key] = data[key][f"{i}"]
                        new_data.append(d)
                    #Saving File
                    with open(f"export//{fname}", 'w') as f:
                        json.dump(new_data, f)
                    pbar.update(1)

    print("\n\nAll Files finished! ")


if __name__ == "__main__":
    main()
