import pandas as pd
from rapidfuzz import fuzz

def find_similar_substrates(df, substrate_name, substrate_col_name='name', n=5):
    # Step 1: Check for case-insensitive exact matches
    df = df.drop(columns=['ec'])
    exact_matches = df[df[substrate_col_name].str.lower() == substrate_name.lower()]
    
    if not exact_matches.empty:
        return exact_matches.drop_duplicates().reset_index(drop=True)
        # return exact_matches[['Name', 'InChIKey']].reset_index(drop=True)

    # Step 2: Compute similarity for all substrates if no exact match is found
    # Step 2: Use RapidFuzz for fast similarity matching
    df['similarity'] = df[substrate_col_name].apply(lambda x: fuzz.ratio(x.lower(), substrate_name.lower()))
 
    # Step 3: Sort by similarity and return the top n matches
    similar_substrates = df.sort_values(by='similarity', ascending=False).head(n)
    
    return similar_substrates.drop_duplicates().reset_index(drop=True)
 # [['Name', 'InChIKey', 'similarity']].reset_index(drop=True)

# Example usage:
# df = pd.DataFrame({
#     'Substrate Name': ['Glucose', 'Fructose', 'Sucrose', 'Lactose', 'Galactose'],
#     'InChIKey': ['WQZGKKKJIJFFOK-UHFFFAOYSA-N', 'BJHIKXHVCXFQLS-UHFFFAOYSA-N', 'COVHOMDLPTBGKH-UHFFFAOYSA-N', 'JHIVVAPYMSGYDF-UHFFFAOYSA-N', 'MLCYWGOTUORCOA-UHFFFAOYSA-N']
# })

if __name__ == "__main__":
    src_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")
    
    results = []
    trials = ['adenosine triphosphate', 'D-fructose-2,6-bisphosphate', '2-Oxoglutaric acid', 'α-ketoglutarate']
    for trial in trials:
        result = find_similar_substrates(src_df, trial, n=3)
        results.append(result)
    result = pd.concat(results)
    print(result)