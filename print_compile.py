# goal: compile many stuff together into one


import os
import pandas as pd


def compile_csvs(paths_to_valids, namespace):
    col = []
    for filepath in paths_to_valids:
        col.append(pd.read_csv(filepath, sep=','))
    df = pd.concat(col)
    if namespace:
        df.to_csv(f"data/_compiled/{namespace}.tsv", sep="\t", index=False)
        
    return df

def compile_jsonls(paths_to_jsonls, namespace):
    # simply concatenate all the lines
    lines = []
    for filepath in paths_to_jsonls:
        with open(filepath, 'r') as f:
            lines.extend(f.readlines())
    if namespace:
        with open(f"data/_compiled/{namespace}.jsonl", 'w') as f:
            f.writelines(lines)


def script0():
    # prepare nonbrenda 
    # "topoff/open", "wos/jbc", 
    # sources = ["scratch/asm", "scratch/hindawi", "scratch/open", "scratch/wiley", 
    #            "topoff/hindawi", "topoff/wiley", 
    #            "wos/asm", "wos/hindawi", "wos/local_shim", "wos/open", "wos/wiley"]
    sources = ["brenda/jbc", "brenda/open", "brenda/pnas", "brenda/scihub", "brenda/wiley"]
    
    # filenames = [f"_valid_{src.replace('/', '-')}-apogee-t2neboth_1.csv" for src in sources]
    # valid_paths = [f"data/valid/{filename}" for filename in filenames]
    # compile_valids(valid_paths, "nonbrenda")
    
    filenames = [f"{src.replace('/', '-')}-apogee-t2neboth_1.jsonl" for src in sources]
    valid_paths = [f"completions/enzy/apogee/{filename}" for filename in filenames]
    
    compile_jsonls(valid_paths, "apogee-brenda")

def script1():
    sources = [filename for filename in os.listdir('data/mbrenda') if filename.endswith('.csv')]
    
    good = []
    for s in sources:
        if s.startswith('_cache_openelse-apogee-t2neboth'):
            if s == '_cache_openelse-apogee-t2neboth_2.csv':
                good.append(s)
            continue
        
        if s.endswith('-apogee-t2neboth_1.csv'):
            good.append(s)
            continue
    compile_csvs([f'data/mbrenda/{s}' for s in good], "apogee-brenda")
    
            
if __name__ == "__main__":
    script0()