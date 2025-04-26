import polars as pl
import os

from enzyextract.pipeline.llm_log import llm_log_schema, read_log
from enzyextract.pipeline.step3_llm_to_df import generate_valid_parquet


if __name__ == "__main__":
    # raise NotImplementedError("This script is only an example.")
    blacklist = whitelist = None

    namespace = 'bench dev2'

    llm_log = read_log('.enzy/llm_log.tsv', separator='\t', schema_overrides=llm_log_schema)
    fpath = '.enzy/completions/bench dev2_20250301.jsonl'
    write_dir = '.enzy/post/valid'

    row = llm_log.filter(pl.col('namespace') == namespace)
    version = row.item(row=0, column='version')
    row = row.filter(pl.col('version') == version)

    structured = row.item(row=0, column='structured')
    compl_fpath = row.item(row=0, column='completion_fpath')
    corresp_fpath = row.item(row=0, column='corresp_fpath')
    write_fpath = os.path.join(write_dir, f"{namespace}_{version}.parquet")

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
