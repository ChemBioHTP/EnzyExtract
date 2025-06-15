import polars as pl
import os
from enzyextract.pipeline.evaluation.eval2_plot_dfs import analyze_correlations

if __name__ == '__main__':
    # raise NotImplementedError("This script is only an example.")

    # working = 'thedata'
    # working = 'pruned'
    # working = 'unpruned'
    # working = 'rumble'
    working = 'thedata'


    # against = 'rumble'
    against = 'brenda'
    # against = 'sabiork'

    scino_only = None
    # scino_only = True
    # scino_only = False
    # scino_only = 'false_revised'

    if scino_only is True:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':
        working += '_no_scientific_revised'
    
    # readme = f'data/matched/EnzymeSubstrate/{against}/{against}_{working}.parquet'
    readme = f'data/metrics/{against}/{against}_{working}.parquet'
    matched_view = pl.read_parquet(readme)

    # matched_view = matched_view.filter(
    #     pl.col('pmid') != '21980421'
    # )
    print("Reading", readme)
    analyze_correlations(matched_view, f"1. {working} 2. {against}")
    