import re
import pandas as pd

# checkpoint df has these columns:
# km_feedback,kcat_feedback,pmid,descriptor,km,kcat,kcat_km,enzyme,substrate,organism,temperature,pH,solvent,other,mutant,enzyme_2,ec_2,ref_2,pH_2,temperature_2,substrate_2,comments_2,km_2,kcat_2,kcat_km_2,mutant_2,

# dists_df has these columns:
# ['pmid', 'distance', 'ident', 'enzyme_name', 'desire', 'target', 'index']
# offset would be index - target
# 'ident' is the sequence

# brenda_df has these columns:
# ec	name	organism_id	organism	accession_type	accessions	

# synonyms_df has these columns:
# ec name synonyms specific_synonyms

# sequence_df eventually has these columns:
# name, short_name, known_name, enzyme_ident, enzyme_offset, amino_acids_checked, sequence

# step 1, turn checkpoint_df into sequence_df
# step 2, anoint sequence_df with known_name via dists_df
# step 3, anoint sequence_df with sequence, etc.
# step 4, 
# step 6, join checkpoint_df with sequence_df


def to_sequence_df(checkpoint_df: pd.DataFrame, brenda_df: pd.DataFrame) -> pd.DataFrame:
    """Step 1: get all the substrates
    If substrate_full is in a row, prefer it. otherwise, use substrate"""
    # sequence_df should start off with these rows:
    # name, short_name, organism, ec, brenda_organism_id, brenda_uniprot
    builder = []
    for i, row in checkpoint_df.iterrows():
        organism = row.get('organism', None)
        ec = row.get('ec_2', None)
        if pd.isna(row['enzyme_full']):
            builder.append((row['enzyme'], None, organism, ec, None, None))
        else:
            builder.append((row['enzyme_full'], row['enzyme'], organism, ec, None, None))
            
        # now brenda's turn
        comments = row.get('comments_2', row.get('descriptor_2', row.get('variant_2', None)))
        # search for the pattern #x#, which indicates the organism
        if comments and pd.notna(comments):
            organism = None
            brenda_uniprot = None
            brenda_organism_id = re.search(r"#\d+#", comments)
            if brenda_organism_id:
                brenda_organism_id = int(brenda_organism_id.group(0).strip("#"))
                # search brenda_df by ec and brenda_organism_id
                # if found, add the organism to the sequence_df
                organism_slice = brenda_df[(brenda_df['ec'] == ec) & (brenda_df['organism_id'] == brenda_organism_id)]
                if not organism_slice.empty:
                    organism = organism_slice.iloc[0]['organism']
                    # and look for brenda uniprot
                    brenda_uniprot = '|'.join(organism_slice['accessions'].dropna()) or None
                
        builder.append((row['enzyme_2'], None, organism, ec, str(brenda_organism_id), brenda_uniprot))
        
    df = pd.DataFrame(builder, columns=['name', 'short_name', 'organism', 'ec', 'brenda_organism_id', 'brenda_uniprot'])
    # drop duplicates
    df = df.drop_duplicates() 
    df = df.dropna(subset=['name']) # we must need the name
    return df

def join_distances(sequence_df, dists_df):
    """Step 2 and 3: anoint sequence_df with known_name and sequences via dists_df"""
    
    name_to_info = {} # dict is faster
    for i, row in dists_df.iterrows():
        name = row['enzyme_name'].lower()
        if name in name_to_info:
            name_to_info[name].append(row)
        else:
            name_to_info[name] = [row]
    
    # now, for each row in sequence_df, find the closest enzyme
    sequence_df['enzyme_ident'] = None
    sequence_df['enzyme_offset'] = None
    sequence_df['amino_acids_checked'] = None
    sequence_df['sequence'] = None
    
    
    for i, row in sequence_df.iterrows():
        if pd.isna(row['name']):
            continue
        name = row['name'].lower()
        if name not in name_to_info:
            name = row['short_name']
            if name:
                name = name.lower()
        
        if name in name_to_info:
            # get the closest enzyme
            closest = name_to_info[name][0]
            sequence_df.at[i, 'enzyme_ident'] = closest['ident']
            sequence_df.at[i, 'enzyme_offset'] = closest['index'] - closest['target']
            sequence_df.at[i, 'sequence'] = closest['sequence']
            
            # calculate amino_acids_checked to be len(closest['desire'].replace('.', ''))
            sequence_df.at[i, 'amino_acids_checked'] = len(closest['desire'].replace('.', ''))
    

def script0():
    # run it all
    checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_rekcat-giveboth-4o_2.csv") # 95/340
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    
    brenda_df = pd.read_csv("fetch_sequences/enzymes/brenda_enzymes.tsv", sep="\t")
    
    # step 1
    sequence_df = to_sequence_df(checkpoint_df, brenda_df)
    
    # step 2,3 anoint sequence_df with info via dists_df
    join_distances(sequence_df, dists_df)
    
    print(sequence_df)

if __name__ == "__main__":
    script0()
    
    
