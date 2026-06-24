"""
Formerly generate_valid.py
"""

import os
from typing import Optional
import pandas as pd
import polars as pl
from enzyextract.pipeline.llm_log import read_log
from enzyextract.post.decode import jsonl_to_decoded_df
from enzyextract.post.metadata.regurgitation import is_content_regurgitated
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
            df.insert(0, 'pmid', pmid)
            df['custom_id'] = custom_id

            if is_content_regurgitated(content):
                df['flag.regurgitation'] = True
            else:
                df['flag.regurgitation'] = None
            


            if df.empty:
                continue
            valids.append(df)
            valid_pmids.add(str(pmid)) # needs to be a set

    stats['total_ingested'] = total_ingested
    stats['valid_pmids'] = len(valid_pmids)
    
    valid_df = clean_columns_for_valid(pd.concat(valids)) # bad units for km and kcat are rejected here
    valid_df = valid_df.astype({'pmid': 'str'})
    
    # detect regurgitation here
    

    if write_fpath: #  and not os.path.exists(write_fpath):
        print("Writing to", write_fpath)
        if write_fpath.endswith('.parquet'):
            pl.from_pandas(valid_df).write_parquet(write_fpath)
        else:
            valid_df.to_csv(write_fpath, index=False)
    
    return valid_df, stats

def namespace_to_parquet(
    namespace: str,
    log_location: str, # e.g. '.enzy/llm_log.tsv'
    write_dir: Optional[str],
) -> pl.DataFrame:
    """
    If `namespace` is unique, then it will read the log file at `log_location` with namespace
    and the latest version, and produce a polars DataFrame.

    If `namespace` is not unique, then unexpected things may occur!

    If `write_dir` is provided, the DataFrame will automatically be written to {write_dir}/{namespace}_{version}.parquet.
    If not provided, it will not write to disk.
    """

    llm_log = read_log(log_location)

    row = llm_log.filter(pl.col('namespace') == namespace)

    if row.select('version').n_unique() > 1:
        print("Warning: namespace has multiple versions. Using the latest version only.")
        row = row.sort('version', descending=True)

    version = row.item(row=0, column='version')
    print("Using namespace:", namespace, "version:", version)

    row = row.filter(pl.col('version') == version)

    structured = row.item(row=0, column='structured')
    compl_fpath = row.item(row=0, column='completion_fpath')
    corresp_fpath = row.item(row=0, column='corresp_fpath')

    if write_dir is not None:
        write_fpath = os.path.join(write_dir, f"{namespace}_{version}.parquet")
    else:
        write_fpath = None

    corresp_df = pl.read_parquet(corresp_fpath)

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

    df, stats = generate_valid_parquet(
        fpath=compl_fpath,
        corresp_df=corresp_df,
        llm_provider='openai',
        write_fpath=write_fpath,
        silence=False,
        use_yaml=not structured
    )
    return df