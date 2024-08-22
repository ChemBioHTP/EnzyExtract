import os
import pandas as pd
from rapidfuzz import fuzz

from kcatextract.fetch_sequences.get_smiles import pubchem_main, rdkit_inchi, rdkit_main

def find_similar_substrates(df, substrate_name, substrate_col_name='name', n=5, fuzzy=True):
    # Step 1: Check for case-insensitive exact matches
    df = df.drop(columns=['ec'])
    exact_matches = df[df[substrate_col_name].str.lower() == substrate_name.lower()]
    
    if not exact_matches.empty:
        result = exact_matches.drop_duplicates().reset_index(drop=True)
        result['similarity'] = 100
        return result
        # return exact_matches[['Name', 'InChIKey']].reset_index(drop=True)
    
    if not fuzzy:
        return df.head(0)
    
    # Step 2: Compute similarity for all substrates if no exact match is found
    # Step 2: Use RapidFuzz for fast similarity matching
    df['similarity'] = df[substrate_col_name].apply(lambda x: fuzz.ratio(x.lower(), substrate_name.lower()))
 
    # Step 3: Sort by similarity and return the top n matches
    similar_substrates = df.sort_values(by='similarity', ascending=False).drop_duplicates().head(n)
    # require: similarity is > 0.5
    similar_substrates = similar_substrates[similar_substrates['similarity'] > 50]
    
    return similar_substrates.reset_index(drop=True)
 # [['Name', 'InChIKey', 'similarity']].reset_index(drop=True)

def chemical_formula_from_inchi(inchi):
    if not inchi.startswith('InChI=1S/'):
        print("Invalid InChI", inchi)
        return None
    return inchi.split('/')[1].split('-')[0]

# Example usage:
# df = pd.DataFrame({
#     'Substrate Name': ['Glucose', 'Fructose', 'Sucrose', 'Lactose', 'Galactose'],
#     'InChIKey': ['WQZGKKKJIJFFOK-UHFFFAOYSA-N', 'BJHIKXHVCXFQLS-UHFFFAOYSA-N', 'COVHOMDLPTBGKH-UHFFFAOYSA-N', 'JHIVVAPYMSGYDF-UHFFFAOYSA-N', 'MLCYWGOTUORCOA-UHFFFAOYSA-N']
# })

def extract_substrate_synonym(doc: str, ai_msg: str):
    # get the substrate
    # expect the string Substrate: <x>
    synonym = None
    for line in doc.split("\n"):
        if line.startswith("Substrate:"):
            # return line.split(":")[1].strip()
            synonym = line.split(":", 1)[1].strip()
            break
        elif not line.strip():
            raise ValueError("invalid format: document should start with Substrate:")
    
    substrate = None
    for line in ai_msg.split("\n"):
        if line.startswith("Final Answer:"):
            substrate = line.split(":", 1)[1].strip()
            if substrate.lower() == 'none':
                substrate = None
            break
    return (substrate, synonym)
    

def obtain_yet_unmatched(df: pd.DataFrame):
    return df[df['known_name'].isna()]
    

# checkpoint df has these columns:
# km_feedback,kcat_feedback,pmid,descriptor,km,kcat,kcat_km,enzyme,substrate,organism,temperature,pH,solvent,other,mutant,enzyme_2,ec_2,ref_2,pH_2,temperature_2,substrate_2,comments_2,km_2,kcat_2,kcat_km_2,mutant_2,

# brenda_inchi_df has these columns:
# name	ec	brenda_id	inchi	chebi	unknown

# smiles_df has these columns:
# Name,Smiles

# inchi_df has these columns:
# Name,Inchi,Smiles

# sequence_df eventually has these columns:
# name, short_name, known_name, inchi, smiles

# step 1, turn checkpoint_df into sequence_df
# step 2, anoint sequence_df with known_name via smiles_df
# step 3, anoint sequence_df with known_name via gpt
# step 4, anoint sequence_df with inchi with known_name and brenda_key
# step 5, anoint sequence_df with smiles with inchi
# step 6, join checkpoint_df with sequence_df

def to_sequence_df(checkpoint_df: pd.DataFrame) -> pd.DataFrame:
    """Step 1: get all the substrates
    If substrate_full is in a row, prefer it. otherwise, use substrate"""
    # substrates = set()
    builder = []
    for i, row in checkpoint_df.iterrows():
        if pd.isna(row['substrate_full']):
            builder.append((row['substrate'], None))
        else:
            builder.append((row['substrate_full'], row['substrate']))
        builder.append((row['substrate_2'], None))
        
    df = pd.DataFrame(builder, columns=['name', 'short_name'])
    # drop duplicates
    df = df.drop_duplicates()
    return df

_greek_to_english = {
    'α': 'alpha',
    'β': 'beta',
    'γ': 'gamma',
    'δ': 'delta',
    'ε': 'epsilon',
    'ζ': 'zeta',
    'η': 'eta',
    'θ': 'theta',
    'ι': 'iota',
    'κ': 'kappa',
    'λ': 'lambda',
    'μ': 'mu',
    'ν': 'nu',
    'ξ': 'xi',
    'ο': 'omicron',
    'π': 'pi',
    'ρ': 'rho',
    'σ': 'sigma',
    'τ': 'tau',
    'υ': 'upsilon',
    'φ': 'phi',
    'χ': 'chi',
    'ψ': 'psi',
    'ω': 'omega'
}

def add_known_names(sequence_df: pd.DataFrame, smiles_df: pd.DataFrame, brenda_df: pd.DataFrame) -> pd.DataFrame:
    """Step 2: anoint sequence_df with known_name via smiles_df, creating a new column"""
    # (case ins)
    sequence_df['known_name'] = None
    smiles_values = set(smiles_df['lower_name'].values)
    brenda_values = set(brenda_df['name'].str.lower().values)
    
    def try_to_find(name):
        if name.lower() in smiles_values:
            return name
        elif name.lower() in brenda_values:
            return name
        return None
    for i, row in sequence_df.iterrows():
        name = row['name']
        if pd.isna(name):
            continue
        # match = smiles_df[smiles_df['lower_name'] == name.lower()]
        # if not match.empty:
        
        # convert greek letter characters to their descriptive names. 
        # bumps 112 
        transformed_name = name
        for greek, english in _greek_to_english.items():
            transformed_name = transformed_name.replace(greek, english)
        
        if try_to_find(transformed_name):
            sequence_df.at[i, 'known_name'] = transformed_name
        else:
            transformed_name = row['short_name']
            if pd.isna(name):
                continue
            if transformed_name:
                for greek, english in _greek_to_english.items():
                    transformed_name = transformed_name.replace(greek, english)
                if try_to_find(transformed_name):
                    sequence_df.at[i, 'known_name'] = transformed_name

def get_yet_unknown_names(sequence_df: pd.DataFrame) -> pd.DataFrame:
    return sequence_df[sequence_df['known_name'].isna()]
    


def latest_smiles_df(fragments_folder='fetch_sequences/results/smiles_fragments') -> tuple[pd.DataFrame, set]:
    """
    Given many fragments, create a single dataframe with all the smiles
    and also return a set for which none were ever found
    """
    # load every csv from data/smiles_fragments/*.tsv
    result = []
    for filename in os.listdir(fragments_folder):
        if filename.endswith('.tsv'):
            df = pd.read_csv(f'{fragments_folder}/{filename}', sep='\t')
        elif filename.endswith('.csv'):
            df = pd.read_csv(f'{fragments_folder}/{filename}')
        else:
            continue
        # if 'InChI' not in df.columns:
            # df['InChI'] = None
        result.append(df[['Name', 'Smiles']])
    out = pd.concat(result)
    # remove all where the Smiles is "not found"
    unqueryable = out[out['Smiles'] == 'not found']['Name'].unique()
    
    out = out[out['Smiles'] != 'not found']
    out = out.drop_duplicates()
    # remove those in unqueryable which are also in out (AKA they were eventually found)
    unqueryable = set(unqueryable) - set(out['Name'].unique())
    out['lower_name'] = out['Name'].str.lower()
    return out, unqueryable

def latest_inchi_df(fragments_folder='fetch_sequences/results/inchi_fragments') -> pd.DataFrame:
    
    result = []
    for filename in os.listdir(fragments_folder):
        if filename.endswith('.tsv'):
            df = pd.read_csv(f'{fragments_folder}/{filename}', sep='\t')
        elif filename.endswith('.csv'):
            df = pd.read_csv(f'{fragments_folder}/{filename}')
        else:
            continue
        assert 'InChI' in df.columns, "Creating InChI dataframe requires InChI column"
        result.append(df[['Name', 'InChI', 'Smiles']])
    return pd.concat(result) # assume all InChIs are valid, barring some unusual circumstance


def drop_same_rows_ignore_case(df, other_columns=['brenda_id', 'inchi']):
    # drop duplicate rows (rows with same name, brenda_id, and inchi) but ignore case
    # but maintain the case sensitivity
    df['lower_name'] = df['name'].str.lower()
    df = df.drop_duplicates(subset=['lower_name'] + other_columns)
    df = df.drop(columns=['lower_name'])
    return df

_timestamp = None
def program_timestamp():
    """Singleton to create a timestamp for the program"""
    global _timestamp
    if _timestamp is None:
        _timestamp = pd.Timestamp.now().strftime("%Y-%m-%d-%H-%M-%S")
    return _timestamp

def redo_smiles_search(unknown_df, fragment_folder='fetch_sequences/results/smiles_fragments'):
    # read the "name" column
    # redo smiles search on
    # save results to fragment_folder, appending the timestamp to create unique filenames
    idents = set(unknown_df['name'])
    if not idents:
        return None, None
    timestamp = program_timestamp()
    # do the search
    
    rdkit_df = rdkit_main(idents)
    
    # get those that are not found in rdkit_df
    idents = rdkit_df[rdkit_df['Smiles'] == 'not found']['Name'].unique()
    pubchem_df = pubchem_main(idents)
    
    if fragment_folder is not None:
        rdkit_df.to_csv(f'{fragment_folder}/rdkit_smiles_{timestamp}.csv', index=False)
        pubchem_df.to_csv(f'{fragment_folder}/pubchem_smiles_{timestamp}.csv', index=False)
    return rdkit_df, pubchem_df
    # save the results
    
def join_inchi(sequence_df, brenda_inchi_df):
    """Step 4: anoint sequence_df with inchi with known_name and brenda_key"""
    
    # brenda_inchi_df has these columns:
    # name	ec	brenda_id	inchi	chebi	unknown
    brenda_inchi_df['lower_name'] = brenda_inchi_df['name'].str.lower()
    
    # join on known_name
    sequence_df['inchi'] = None
    
    # maybe a dict would be faster?
    name_to_inchi = dict(zip(brenda_inchi_df['lower_name'], brenda_inchi_df['inchi']))
    for i, row in sequence_df.iterrows():
        name = row['known_name']
        if pd.isna(name):
            continue
        # match = brenda_inchi_df[brenda_inchi_df['lower_name'] == name.lower()]
        inchi = name_to_inchi.get(name.lower())
        # if not match.empty:
        if inchi:
            # sequence_df.at[i, 'inchi'] = match['inchi'].values[0]
            sequence_df.at[i, 'inchi'] = inchi


def join_smiles(sequence_df: pd.DataFrame, inchi_df, request_missing=False):
    """Step 5: anoint sequence_df with smiles via inchi
    This requires an API call"""
    
    if request_missing:
        # get smiles that are not found in inchi_df
        idents = []
        for i, row in sequence_df.iterrows():
            inchi = row['inchi']
            if pd.isna(inchi):
                continue
            if inchi not in inchi_df['InChI'].values:
                idents.append((row['known_name'], inchi))
        if idents:
            idents = pd.DataFrame(idents, columns=['known_name', 'inchi'])
            idents = idents.drop_duplicates()
            
            timestamp = program_timestamp()
            # do the search
            add_df = rdkit_inchi(idents['known_name'], idents['inchi'])
            add_df.to_csv(f'fetch_sequences/results/inchi_fragments/rdkit_{timestamp}.csv', index=False)
            inchi_df = pd.concat([inchi_df, add_df])
    
    # join on inchi
    sequence_df['smiles'] = None
    inchi_to_smiles = dict(zip(inchi_df['InChI'], inchi_df['Smiles']))
    for i, row in sequence_df.iterrows():
        inchi = row['inchi']
        if pd.isna(inchi):
            continue
        smiles = inchi_to_smiles.get(inchi)
        if smiles and smiles != 'not found':
            sequence_df.at[i, 'smiles'] = smiles

def join_sequence_df(checkpoint_df, sequence_df):
    """Step 6: join checkpoint_df with sequence_df"""
    # join by sequence_df's known_name to substrate_2
    # if not (ie. column substrate_2 doesn't exist or is null), then to substrate
    # if not, then to substrate_full
    
    # if 'substrate_2' in checkpoint_df.columns:
    #     result_df = pd.merge(checkpoint_df, sequence_df, left_on='substrate_2', right_on='name', how='left')
    # result_df = pd.merge(checkpoint_df, sequence_df, left_on='substrate', right_on='name', how='left')
    # result_df = pd.merge(checkpoint_df, sequence_df, left_on='substrate_full', right_on='name', how='left')
    
    checkpoint_df['inchi'] = None
    checkpoint_df['smiles'] = None
    sequence_to_idents = dict(zip(sequence_df['known_name'].str.lower(), zip(sequence_df['inchi'], sequence_df['smiles'])))
    for i, row in checkpoint_df.iterrows():
        name = row.get('substrate_2')
        if pd.isna(name):
            name = row['substrate']
            if pd.isna(name):
                name = row['substrate_full']
                if pd.isna(name):
                    continue
        # case insensitve
        inchi, smiles = sequence_to_idents.get(name.lower(), (None, None))
        checkpoint_df.at[i, 'inchi'] = inchi
        checkpoint_df.at[i, 'smiles'] = smiles
        
    
    # expect len(result_df) == len(checkpoint_df)
    
    return checkpoint_df
    
    

def script_minus1():
    # remove EC from src_df to reduce dupes
    src_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")
    # src_df = src_df.drop(columns=['ec'])
    # src_df = src_df.drop_duplicates()
    
    src_df = drop_same_rows_ignore_case(src_df)
    src_df.to_csv("fetch_sequences/results/smiles/brenda_inchi_subs.tsv", sep="\t", index=False)
    # halves the size

def script0():
    """Objective: can we get matches for these synonyms?"""
    src_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_subs.tsv", sep="\t")
    
    results = []
    # trials = ['adenosine triphosphate', 'D-fructose-2,6-bisphosphate', '2-Oxoglutaric acid', 'α-ketoglutarate']
    trials = ['p-NPP'] # 'Nicotinamide adenine dinucleotide (reduced)']
    for trial in trials:
        result = find_similar_substrates(src_df, trial, n=20)
        results.append(result)
    result = pd.concat(results)
    print(result)

def infuse_with_substrates(checkpoint_df=None, redo_smiles=False, redo_inchi=False):
    """Objective: can we see what is left to be matched?"""
    
    
    # checkpoint df has these columns:
    # km_feedback,kcat_feedback,pmid,descriptor,km,kcat,kcat_km,enzyme,substrate,organism,temperature,pH,solvent,other,mutant,enzyme_2,ec_2,ref_2,pH_2,temperature_2,substrate_2,comments_2,km_2,kcat_2,kcat_km_2,mutant_2,

    # brenda_inchi_df has these columns:
    # name	ec	brenda_id	inchi	chebi	unknown

    # smiles_df has these columns:
    # Name,InChI,Smiles

    # step 1, turn checkpoint_df into sequence_df
    # step 2, anoint sequence_df with known_name via smiles_df
    # step 3, anoint sequence_df with known_name via gpt
    # step 4, anoint sequence_df with inchi with known_name and brenda_key
    # step 5, anoint sequence_df with smiles via inchi
    # step 6, join checkpoint_df with sequence_df
    
    if checkpoint_df is None:
        checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_rekcat-giveboth-4o_2.csv") # 95/340
    # checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_brenda-rekcat-md-v1-2_1.csv") # 497/2322 known
    
    brenda_substrate_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")
    
    # step 1
    sequence_df = to_sequence_df(checkpoint_df)
    sequence_df= drop_same_rows_ignore_case(sequence_df, other_columns=['short_name'])
    smiles_df, unqueryable = latest_smiles_df()
    
    # step 2
    add_known_names(sequence_df, smiles_df, brenda_substrate_df)
    unknown_df = get_yet_unknown_names(sequence_df)
    
    queryable = unknown_df[~unknown_df['name'].isin(unqueryable)]
    
    if redo_smiles:
        df1, df2 = redo_smiles_search(queryable)
        if df1 is not None:
            smiles_df = pd.concat([smiles_df, df1])
        if df2 is not None:
            smiles_df = pd.concat([smiles_df, df2])
    print(unknown_df)
    
    # step 3: skip
    
    # step 4
    inchi_df = latest_inchi_df()
    join_inchi(sequence_df, brenda_substrate_df)

    # step 5
    join_smiles(sequence_df, inchi_df, request_missing=redo_inchi)
    
    # print(sequence_df)
    
    substrated_df = join_sequence_df(checkpoint_df, sequence_df)
    
    # print(substrated_df)
    return substrated_df
    
    
if __name__ == "__main__":
    infuse_with_substrates()