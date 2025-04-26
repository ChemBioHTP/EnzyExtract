import polars as pl
import os
from enzyextract.pipeline.step5b_compare_precision_recall import gpt_dataframe, load_runeem_df, main
if __name__ == '__main__':
    # raise NotImplementedError("This script is only an example.")
    
    # exit(0)
    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    # working = 'sabiork'
    # working = 'bucket'
    # working = 'apatch'
    # working = 'everything'
    working = 'thedata'

    against = 'runeem'
    # against = 'brenda'
    # against = 'sabiork'

    # scino_only = None
    # scino_only = True
    scino_only = False
    # scino_only = 'false_revised'

    whitelist = None
    # whitelist = 'wide_tables_only'
    # whitelist = 'hallucinated_micro'

    # step 2: matching
    gpt_df = gpt_dataframe(working)

    if scino_only is True:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':
        working += '_no_scientific_revised'
    
    is_brenda = False
    if against == 'runeem':
        known_df = load_runeem_df(exclude_train=True)
    elif against == 'sabiork':
        known_df = pl.read_parquet('data/sabiork/valid_sabiork.parquet')
    else:
        known_df = None
        is_brenda = True


    matched_view = main(
        working=working,
        against_known=against,
        scino_only=scino_only,
        whitelist=whitelist,
        gpt_df=gpt_df,
        known_df=known_df,
        is_brenda=is_brenda,
    )
    fdir = f'data/metrics/{against}'
    os.makedirs(fdir, exist_ok=True)
    pass