import pandas as pd
from kcatextract.fetch_sequences.get_closest_substrate import find_similar_substrates

# substrate

def drop_same_rows_ignore_case(df):
    # drop duplicate rows (rows with same name, brenda_id, and inchi) but ignore case
    # but maintain the case sensitivity
    df['name_lower'] = df['name'].str.lower()
    df = df.drop_duplicates(subset=['name_lower', 'brenda_id', 'inchi'])
    df = df.drop(columns=['name_lower'])
    return df

if __name__ == "__main__":
    src_df = pd.read_csv("C:/conjunct/table_eval/fetch_sequences/substrates/brenda_inchi_all.tsv", sep="\t")
    # lower every single name
    src_df = drop_same_rows_ignore_case(src_df)
    
    results = []
    trials = ['Dns-PhgRAPW'] # 'adenosine triphosphate', 'D-fructose-2,6-bisphosphate', '2-Oxoglutaric acid', 'α-ketoglutarate', '1,2-ethanediol', 
            #   '2-trans-hexadecenoyl-CoA', 'benzoyl-CoA', "Pyridoxal 5'-phosphate", 'synthetic peptide']
              # '5a-androstane-3a,17β-diol']
    for trial in trials:
        result = find_similar_substrates(src_df, trial, n=3)
        results.append(result)
    result = pd.concat(results)
    print(result)