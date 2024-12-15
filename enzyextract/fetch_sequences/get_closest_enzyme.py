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
# name, short_name, enzyme_ident, enzyme_offset, amino_acids_checked, sequence

# step 1, turn checkpoint_df into sequence_df
# step 2, anoint sequence_df with known_name via dists_df
# step 3, anoint sequence_df with sequence, etc.
# step 4, 
# step 6, join checkpoint_df with sequence_df

def preferred_name(sequence_row, name_to_info):
    # look at name, then short_name, then 
    if 'enzyme' in sequence_row:
        # this is actually a checkpoint_row
        name = sequence_row['enzyme']
        if name and not pd.isna(name):
            name = name.lower()
        if name in name_to_info:
            return name
        if 'enzyme_full' in sequence_row:
            name = sequence_row['enzyme_full']
            if name and not pd.isna(name):
                name = name.lower()
            if name in name_to_info:
                return name
    else:
        name = sequence_row['name'].lower()
        if name in name_to_info:
            return name
        name = sequence_row['short_name']
        if name:
            name = name.lower()
            if name in name_to_info:
                return name
    return None


def get_name_to_info_for_dists(dists_chunk):
    name_to_info = {}
    for i, row in dists_chunk.iterrows():
        name = row['enzyme_name'].lower()
        if name in name_to_info:
            name_to_info[name].append(row)
        else:
            name_to_info[name] = [row]
    return name_to_info

def get_name_to_info_for_sequence(sequence_chunk):
    name_to_info = {}
    for i, row in sequence_chunk.iterrows():
        name = row['name'].lower()
        if name in name_to_info:
            name_to_info[name].append(row)
        else:
            name_to_info[name] = [row]
    return name_to_info

def to_sequence_df(checkpoint_df: pd.DataFrame, brenda_df: pd.DataFrame) -> pd.DataFrame:
    """Step 1: get all the substrates
    If substrate_full is in a row, prefer it. otherwise, use substrate"""
    # sequence_df should start off with these rows:
    # pmid, name, short_name, organism, ec, brenda_organism_id, brenda_uniprot
    builder = []
    for i, row in checkpoint_df.iterrows():
        organism = row.get('organism', None)
        ec = row.get('ec_2', None)
        pmid = row['pmid']
        if pd.isna(row['enzyme_full']):
            builder.append((pmid, row['enzyme'], None, organism, ec, None, None))
        else:
            builder.append((pmid, row['enzyme_full'], row['enzyme'], organism, ec, None, None))
            
        # now brenda's turn
        comments = row.get('comments_2', row.get('descriptor_2', row.get('variant_2', None)))
        # search for the pattern #x#, which indicates the organism
        brenda_organism_id = None
        brenda_uniprot = None
        if comments and pd.notna(comments):
            organism = None
            brenda_organism_id = re.search(r"#\d+#", comments)
            if brenda_organism_id:
                brenda_organism_id = brenda_organism_id.group(0).strip("#")
                # search brenda_df by ec and brenda_organism_id
                # if found, add the organism to the sequence_df
                organism_slice = brenda_df[(brenda_df['ec'] == ec) & (brenda_df['organism_id'] == int(brenda_organism_id))]
                if not organism_slice.empty:
                    organism = organism_slice.iloc[0]['organism']
                    # and look for brenda uniprot
                    brenda_uniprot = '|'.join(organism_slice['accessions'].dropna()) or None
                
        builder.append((pmid, row['enzyme_2'], None, organism, ec, brenda_organism_id, brenda_uniprot))
        
    df = pd.DataFrame(builder, columns=['pmid', 'name', 'short_name', 'organism', 'ec', 'brenda_organism_id', 'brenda_uniprot'])
    # drop duplicates
    df = df.drop_duplicates() 
    df = df.dropna(subset=['name']) # we must need the name
    return df

def join_distances(sequence_df, dists_df):
    """Step 2 and 3: anoint sequence_df with known_name and sequences via dists_df
    
    It's easiest to do this by pmid
    """
    
    # name_to_info = {} # dict is faster
    # for i, row in dists_df.iterrows():
    #     name = row['enzyme_name'].lower()
    #     if name in name_to_info:
    #         name_to_info[name].append(row)
    #     else:
    #         name_to_info[name] = [row]
    
    # now, for each row in sequence_df, find the closest enzyme
    sequence_df['enzyme_ident'] = None
    sequence_df['enzyme_offset'] = None
    sequence_df['amino_acids_checked'] = None
    sequence_df['sequence'] = None
    sequence_df['sequence_name'] = None
    
    sequence_df['sequence_desired'] = None
    sequence_df['sequence_desired_at'] = None
    
    def fill_row_with(df, i, closest):
        if pd.notna(closest['sequence']):
            # df.loc: we need the orig index
            df.at[i, 'enzyme_ident'] = closest['ident']
            df.at[i, 'enzyme_offset'] = closest['index'] - closest['target']
            df.at[i, 'sequence'] = closest['sequence']
            df.at[i, 'amino_acids_checked'] = len(closest['desire'].replace('.', ''))
            df.at[i, 'sequence_name'] = closest['enzyme_name']
        df.at[i, 'sequence_desired'] = closest['desire']
        df.at[i, 'sequence_desired_at'] = closest['target']
    
    
    
    for pmid in sequence_df['pmid'].unique():
        sequence_chunk = sequence_df[sequence_df['pmid'] == pmid]
        dists_chunk = dists_df[dists_df['pmid'] == pmid]
        # if unique enzymes in sequence_chunk is 1, then we can just use the first one of dists_chunk
        if len(sequence_chunk['name'].unique()) == 1:
            if not dists_chunk.empty:
                
                assert len(dists_chunk['enzyme_name'].unique()) <= 1, "Unique enzymes per pmid in checkpoint should imply unique enzymes in distances (since only through gpt is enzyme_name nonnull)"
                
                # name = sequence_chunk['name'].iloc[0].lower()
                sequence_name = sequence_chunk['name'].iloc[0]
                dists_name = dists_chunk['enzyme_name'].iloc[0]
                if sequence_name.lower() != dists_name.lower():
                    print("Mismatched names", sequence_name, dists_name)
                # assert sequence_name.lower() == dists_name.lower() # based on how the df is constructed, should be true
                closest = dists_chunk.iloc[0]
                for i, row in sequence_chunk.iterrows():
                    fill_row_with(sequence_df, i, closest)
                    #
                continue
        
        # search by name
        name_to_info = get_name_to_info_for_dists(dists_chunk)
        
        for i, row in sequence_chunk.iterrows():
            name = preferred_name(row, name_to_info)
            if name:
                # get the closest enzyme
                closest = name_to_info[name][0]
                fill_row_with(sequence_df, i, closest)
    
    # for i, row in sequence_df.iterrows():
    #     if pd.isna(row['name']):
    #         continue
    #     name = row['name'].lower()
    #     if name not in name_to_info:
    #         name = row['short_name']
    #         if name:
    #             name = name.lower()
        
    #     if name in name_to_info:
    #         # get the closest enzyme
    #         closest = name_to_info[name][0]
    #         target = closest['target']
    #         if target != -1:
    #             sequence_df.at[i, 'enzyme_ident'] = closest['ident']
    #             sequence_df.at[i, 'enzyme_offset'] = closest['index'] - closest['target']
    #             sequence_df.at[i, 'sequence'] = closest['sequence']
                
    #             # calculate amino_acids_checked to be len(closest['desire'].replace('.', ''))
    #             sequence_df.at[i, 'amino_acids_checked'] = len(closest['desire'].replace('.', ''))
    #         sequence_df.at[i, 'sequence_desired'] = closest['desire']
    #         sequence_df.at[i, 'sequence_desired_at'] = closest['target']

def join_sequence_df(checkpoint_df, sequence_df):
    columns = ['brenda_organism_id', 'brenda_uniprot', 'enzyme_ident', 'enzyme_offset', 'amino_acids_checked', 'sequence', 'sequence_desired', 'sequence_desired_at']
    for col in columns:
        checkpoint_df[col] = None
    for i, row in checkpoint_df.iterrows():
        pmid = row['pmid']
        sequence_chunk = sequence_df[sequence_df['pmid'] == pmid]
        if sequence_chunk.empty:
            continue 
        # there can be multiple, match using the enzyme name
        name_to_info = get_name_to_info_for_sequence(sequence_chunk)
        name = preferred_name(row, name_to_info)
        if name:
            closest = name_to_info[name][0]
            for col in columns:
                checkpoint_df.at[i, col] = closest[col]
        

def script0():
    # run it all
    checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_rekcat-giveboth-4o_2.csv") # 95/340
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    
    brenda_df = pd.read_csv("fetch_sequences/enzymes/brenda_enzymes.tsv", sep="\t")
    
    # step 1
    sequence_df = to_sequence_df(checkpoint_df, brenda_df)
    
    # step 2,3 anoint sequence_df with info via dists_df
    join_distances(sequence_df, dists_df)
    
    # step 4, join checkpoint_df with sequence_df
    join_sequence_df(checkpoint_df, sequence_df)
    
    print(sequence_df)

if __name__ == "__main__":
    script0()
    
    
