import polars as pl
import os
from enzyextract.pipeline.step6_plot_dfs import analyze_correlations

if __name__ == '__main__':
    # raise NotImplementedError("This script is only an example.")




    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    # working = 'sabiork'
    # working = 'apatch'
    # working = 'bucket'
    # working = 'everything'
    working = 'thedata'

    # against = 'runeem'
    against = 'brenda'
    # against = 'sabiork'

    # scino_only = True
    scino_only = False
    # scino_only = None
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
    print(readme)
    analyze_correlations(matched_view, f"1. {working} 2. {against}")
    