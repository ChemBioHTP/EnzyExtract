"""
Formerly generate_valid.py
"""

import json
import os
import pandas as pd
import polars as pl
from enzyextract.submit.batch_decode import jsonl_to_decoded_df
from enzyextract.submit.batch_utils import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.hungarian.csv_fix import clean_columns_for_valid


def generate_valid_parquet(fpath,
    *,
    corresp_df = None, 
    llm_provider = 'openai',
    write_fpath = None, # write destination
    silence = True,
    use_yaml=True,
) -> pl.DataFrame:
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

    
    # streamed_content = get_batch_output(fpath)
    decoded_df = jsonl_to_decoded_df(fpath, llm_provider=llm_provider, corresp_df=corresp_df)
    streamed_content = (
        decoded_df
        .select('custom_id', 'content', 'finish_reason', 'pmid')
        # .select('custom_id', 'content', 'finish_reason', 'pmid', 'all_txt')
        .iter_rows()
    )

    # streamed_content = streamed_content
    
    for custom_id, content, finish_reason, pmid in streamed_content:
        # pmid = str(pmid_from_usual_cid(custom_id))
        # pmid = custom_id.rsplit('_', 1)[-1]
        
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
