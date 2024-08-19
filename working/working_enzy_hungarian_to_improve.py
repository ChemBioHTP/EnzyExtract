import pandas as pd
from kcatextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from kcatextract.hungarian.hungarian_matching import match_dfs_by_pmid
from kcatextract.hungarian.postmatched_utils import convenience_rearrange_cols
from kcatextract.utils.construct_batch import merge_2_yamls, yaml_to_df, fix_multiple_yamls
from kcatextract.utils.fresh_version import next_available_version


YAMLS_ORIG = "completions/enzy/brenda-rekcat-md-v1-2_1.md"
YAMLS_IMPROVE = "completions/enzy_improve/brenda-rekcat-partB_1.md"

# YAMLS_B = "completions/enzy/brenda-rekcat-md-mini_1.md"

dest_folder = "completions/enzy_improve"
namespace = "rekcat-tableless" # "rekcat-mini" # 
version = next_available_version(dest_folder, namespace, '.csv')
print("Using version", version)

# DEST = f"{dest_folder}/{namespace}_{version}.md"
DEST_CSV = f"{dest_folder}/{namespace}_{version}.csv"

# fix multiple yamls was here
    

def process_file(base_path, improved_path, auto_context=True):
    dfs = []
    # with open(file_path, 'r', encoding='utf-8') as f:
        # for yaml, pmid in extract_yaml_code_blocks(f.read()):
    bases = {k: v for k, v in fix_multiple_yamls(file_path=base_path)}
    
    for pmid, improvement in fix_multiple_yamls(file_path=improved_path):
        based = bases.get(pmid, "")
        if based:
            continue # for now, just look at those with no base (ie. no tables detected)
        
        yaml = merge_2_yamls(based, improvement, debugpmid=pmid)
        if not yaml:
            continue # for now, skip bad yamls
        df, context = yaml_to_df(yaml, auto_context=auto_context)
        if auto_context:
            df = widen_df(df, brenda=False)
        if not context:
            print("Warning: no context found for PMID", pmid)
        df['pmid'] = pmid
        columns = ['descriptor', 'kcat', 'km', 'pmid', 'kcat_km']
        if auto_context:
            columns += ['enzyme',
            'substrate',
            'mutant',
            'organism',
            'temperature',
            'pH',
            # 'solvent',
            'solution',
            'other']
        if df.empty:
            continue # this keeps happening
        if not all(col in df.columns for col in columns):
            raise ValueError(f"Columns not found in {df.columns} in PMID {pmid}")
        dfs.append(df[columns])
    return pd.concat(dfs)
df_a = prep_for_hungarian(process_file(YAMLS_ORIG, YAMLS_IMPROVE)) 
# convert_df() is needed: still need to standardize, ie. removing µ characters with micro and so on
# df_b = process_file(YAMLS_B)

# use brenda for df_b
df_b = prep_for_hungarian(pd.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'))
df_b = widen_df(df_b)

pmids_a = set(df_a['pmid'])
pmids_b = set(df_b['pmid'])
print("PMIDs in A but not in B", pmids_a - pmids_b)
# print("PMIDs in B but not in A", pmids_b - pmids_a)

# use the sorted union
pmids = sorted(pmids_a)
df_c = match_dfs_by_pmid(df_a, df_b, pmids, coefficients={
    # ignore descriptor
    "kcat": 0.8,
    "km": 0.5,
    "substrate": 0.2,
    "mutant": 1, # mutants are rare but probably reliable
})

# rearrange columns for convenience:
df_c = convenience_rearrange_cols(df_c)

df_c.to_csv(DEST_CSV, index=False)
# with open(DEST, 'w', encoding='utf-8') as f:
    # f.write(df_c.to_markdown(index=False))


