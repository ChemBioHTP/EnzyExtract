
import json
import os
import pandas as pd
from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.hungarian.csv_fix import clean_columns_for_valid


def generate_valid_parquet(namespace, # tuned # tableless-oneshot # brenda-rekcat-md-v1-2
        *,
        version = None, # None
        compl_folder = 'completions/enzy', # C:/conjunct/table_eval/completions/enzy
        silence = True,
        use_yaml=True,
        use_cached=True
):
    """
    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    
    filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
    print(f"Located {filename} version {version} in {compl_folder}")
    
    validated_csv = f'data/valid/_valid_{namespace}_{version}.parquet'
    
    valids = []
    valid_pmids = set()
    total_ingested = 0
    
    stats = {}
    stats['namespace'] = namespace
    print("Namespace:", namespace)

    if use_cached and validated_csv and os.path.exists(validated_csv):
        print("Already exists. Using cached valid_csv.")
        return pd.read_csv(validated_csv), {}
    
    for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
        pmid = str(pmid_from_usual_cid(custom_id))
        
        content = content.replace('\nextras:\n', '\ndata:\n') # blunder
        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        total_ingested += 1
        
        if use_yaml:
            _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid))
        else:
            # assume json content
            _generator = [(0, equivalent_from_json_schema(content))]
        for _, yaml in _generator: # 
            
            df, context = yaml_to_df(yaml, auto_context=True, debugpmid=None if silence else pmid) # pmid, silence debug
            df['pmid'] = pmid
            if df.empty:
                continue
            valids.append(df)
            valid_pmids.add(str(pmid)) # needs to be a set

    stats['total_ingested'] = total_ingested
    stats['valid_pmids'] = len(valid_pmids)
    
    valid_df = clean_columns_for_valid(pd.concat(valids)) # bad units for km and kcat are rejected here
    valid_df = valid_df.astype({'pmid': 'str'})
    
    if validated_csv and not os.path.exists(validated_csv):
        print("Writing to", validated_csv)
        if validated_csv.endswith('.parquet'):
            import polars as pl
            pl.from_pandas(valid_df).write_parquet(validated_csv)
        else:
            valid_df.to_csv(validated_csv, index=False)
    
    return valid_df, stats

if __name__ == "__main__":
    blacklist = whitelist = None

    namespace = 'cherry-dev-manifold'

    structured = namespace.endswith('-str')
    version = None
    compl_folder = 'completions/enzy'

    # merge fragments
    # check to see if we need to merge
    need_merge = False
    filename, version = locate_correct_batch(compl_folder, namespace)
    if filename.endswith('0.jsonl'):
        need_merge = True
    
    if need_merge:
        from enzyextract.utils.openai_management import merge_chunked_completions
        print(f"Merging all chunked completions for {filename} v{version}. Confirm? (y/n)")    
        if input() != 'y':
            exit(0)
        merge_chunked_completions(namespace, version=version, compl_folder=compl_folder, dest_folder=compl_folder)

    df, stats = generate_valid_parquet(namespace=namespace, version=version, compl_folder=compl_folder, 
              silence=False,
              use_yaml=not structured)
