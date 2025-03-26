"""
Formerly generate_valid.py
"""

import json
import os
import pandas as pd
import polars as pl
from enzyextract.submit.batch_utils import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.hungarian.csv_fix import clean_columns_for_valid


def generate_valid_parquet(fpath,
    *,
    write_fpath = None, # write destination
    silence = True,
    use_yaml=True,
):
    """
    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    
    
    assert write_fpath
    write_dir = os.path.dirname(write_fpath)
    os.makedirs(write_dir, exist_ok=True)
    
    valids = []
    valid_pmids = set()
    total_ingested = 0
    
    stats = {}
    
    for custom_id, content, finish_reason in get_batch_output(fpath):
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
    
    if write_fpath: #  and not os.path.exists(write_fpath):
        print("Writing to", write_fpath)
        if write_fpath.endswith('.parquet'):
            import polars as pl
            pl.from_pandas(valid_df).write_parquet(write_fpath)
        else:
            valid_df.to_csv(write_fpath, index=False)
    
    return valid_df, stats

if __name__ == "__main__":
    raise NotImplementedError("This script is only an example.")
    from enzyextract.pipeline.step1_run_tableboth import llm_log_schema_overrides
    blacklist = whitelist = None

    namespace = 'bench dev2'

    llm_log = pl.read_csv('.enzy/llm_log.tsv', separator='\t', schema_overrides=llm_log_schema_overrides)
    fpath = '.enzy/completions/bench dev2_20250301.jsonl'
    write_dir = '.enzy/post/valid'

    row = llm_log.filter(pl.col('namespace') == namespace)
    version = row.item(row=0, column='version')
    row = row.filter(pl.col('version') == version)

    structured = row.item(row=0, column='structured')
    compl_fpath = row.item(row=0, column='completion_fpath')
    write_fpath = os.path.join(write_dir, f"{namespace}_{version}.parquet")

    # merge fragments
    # check to see if we need to merge
    need_merge = row.height > 1
    if need_merge:
        compl_folder = os.path.dirname(compl_fpath)
        filename = os.path.basename(compl_fpath)

        from enzyextract.submit.openai_management import merge_chunked_completions
        print(f"Merging all chunked completions for {filename} v{version}. Confirm? (y/n)")    
        if input() != 'y':
            exit(0)
        merge_chunked_completions(namespace, version=version, compl_folder=compl_folder, dest_folder=compl_folder)

    df, stats = generate_valid_parquet(fpath=compl_fpath,
              write_fpath=write_fpath,
              silence=False,
              use_yaml=not structured)
