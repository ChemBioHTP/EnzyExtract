from typing import Callable
import polars as pl
import os

def read_all_dfs(folderpath, blacklist: Callable[[str], bool] = None, so=None) -> pl.DataFrame:
    """
    Reads all dataframes in a folder, then concatenates them.
    """
    dfs = []
    for filename in os.listdir(folderpath):
        if blacklist is not None and blacklist(filename):
            continue
        if filename.endswith('.tsv'):
            df = pl.read_csv(f'{folderpath}/{filename}', separator='\t', schema_overrides=so)
            dfs.append(df)
        elif filename.endswith('.csv'):
            df = pl.read_csv(f'{folderpath}/{filename}', schema_overrides=so)
            dfs.append(df)
        elif filename.endswith('.parquet'):
            df = pl.read_parquet(f'{folderpath}/{filename}')
            dfs.append(df)
    if not dfs:
        return None
    return pl.concat(dfs, how='diagonal')