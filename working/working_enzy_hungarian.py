import pandas as pd
from enzyextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.utils.yaml_process import YamlVersions
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.yaml_process import fix_multiple_yamls, yaml_to_df


YAMLS_A = "completions/enzy_tuned/tableless-oneshot-mini_1.md"
# "completions/enzy/brenda-rekcat-md-v1-2_1.md"

# YAMLS_B = "completions/enzy/brenda-rekcat-md-mini_1.md"

dest_folder = "completions/enzy_tuned"
namespace = "tableless-oneshot-mini" # "rekcat-vs-brenda"
version = next_available_version(dest_folder, namespace, '.csv')
print("Using version", version)

dest_csv = f"{dest_folder}/{namespace}_{version}.csv"

# fix multiple yamls was here
YAML_VERSION = YamlVersions.ONESHOT    

def process_file(file_path, auto_context=True):
    dfs = []
    # with open(file_path, 'r', encoding='utf-8') as f:
        # for yaml, pmid in extract_yaml_code_blocks(f.read()):
    for pmid, yaml in fix_multiple_yamls(file_path=file_path):
        df, context = yaml_to_df(yaml, auto_context=auto_context, version=YAML_VERSION, debugpmid=pmid)
        if df.empty:
            continue
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
df_a = prep_for_hungarian(process_file(YAMLS_A)) # still need to standardize, ie. removing µ characters with micro and so on
# df_b = process_file(YAMLS_B)

# use brenda for df_b
df_b = prep_for_hungarian(pd.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'))
df_b = widen_df(df_b)

pmids_a = set(df_a['pmid'])
pmids_b = set(df_b['pmid'])
print("PMIDs in A but not in B", pmids_a - pmids_b)

# use the sorted union
# pmids = sorted(pmids_a.union(pmids_b))
pmids = sorted(pmids_a)
df_c = match_dfs_by_pmid(df_a, df_b, pmids, coefficients={
    # ignore descriptor
    "kcat": 0.8,
    "km": 0.5,
    "substrate": 0.2,
    "mutant": 1, # mutants are rare but probably reliable
})

# write df_c (markdown) to file
df_c.to_csv(dest_csv, index=False)
# with open(DEST, 'w', encoding='utf-8') as f:
    # f.write(df_c.to_markdown(index=False))


