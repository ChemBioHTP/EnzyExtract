import os
import re

from Bio.Data.IUPACData import protein_letters_3to1_extended
from sklearn.metrics import mean_absolute_error, mean_squared_error

from enzyextract.metrics.precision_recall import asof_precision_recall, exact_precision_recall, extract_value_and_unit_df
from enzyextract.pipeline.step5_compare_dfs import _remove_bad_es_calc_kcat_value_and_clean_mutants, gpt_dataframe, load_runeem_df
from enzyextract.thesaurus.mutant_patterns import amino3
from datetime import datetime
import polars as pl
import polars.selectors as cs
import rapidfuzz

from enzyextract.hungarian.hungarian_matching import is_wildtype
from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit
from enzyextract.hungarian import pl_hungarian_match
from enzyextract.thesaurus.mutant_patterns import mutant_pattern, mutant_v3_pattern




def main(
    working: str,
    against_known: str,
    scino_only: str,
    whitelist: str,
    gpt_df: pl.DataFrame, # the unknown df
    known_df: pl.DataFrame, # the known df
    is_brenda: bool = False,
):
    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}




    

    # exclude scientific notation: exclude "10^" to see if it improves acc like I think
    
    if scino_only is True:
        gpt_df = gpt_df.filter(
            pl.col('kcat').str.contains('10\^')
            | pl.col('km').str.contains('10\^')
        )
        working += '_scientific_notation'
    elif scino_only is False:
        gpt_df = gpt_df.filter(
            (
                ~pl.col('kcat').str.contains('10\^')
                | pl.col('kcat').is_null()
            ) & (
                ~pl.col('km').str.contains('10\^')
                | pl.col('km').is_null()
            )
        )
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':

        bad_pmids = pl.read_parquet('data/revision/apogee-revision.parquet').filter(
            pl.col('kcat_scientific_notation')
        )
        gpt_df = gpt_df.filter(~pl.col('pmid').is_in(bad_pmids['pmid']))
        gpt_df = gpt_df.filter(
            ~pl.col('kcat').str.contains('10\^')
            & ~pl.col('km').str.contains('10\^')
        )
        working += '_no_scientific_revised'

    
    if whitelist is not None:
        if whitelist == 'hallucinated_micro':
            pmids_df = pl.read_parquet('data/pmids/apogee_hallucinated_micro.parquet')
        elif whitelist == 'wide_tables_only':
            pmids_df = pl.read_parquet('data/pmids/apogee_wide_tables_7plus.parquet')
        else:
            raise ValueError("Invalid whitelist")
        pmids = set(pmids_df['pmid'].unique())
        known_df = known_df.filter(pl.col('pmid').is_in(pmids))
        gpt_df = gpt_df.filter(pl.col('pmid').is_in(pmids))
        working += '_' + whitelist

    # why is it not unique?
    wanted_pmids = set(known_df['pmid'].unique())
    gpt_df = gpt_df.filter(pl.col('pmid').is_in(wanted_pmids))
    gpt_df = gpt_df.unique(maintain_order=True)
    known_df = known_df.unique(maintain_order=True)


    kcat_df = extract_value_and_unit_df(gpt_df, 'kcat').drop('kcat.value', 'kcat.unit')
    km_df = extract_value_and_unit_df(gpt_df, 'km').drop('km.value', 'km.unit')
    known_kcat_df = extract_value_and_unit_df(known_df, 'kcat').drop('kcat.value', 'kcat.unit')
    known_km_df = extract_value_and_unit_df(known_df, 'km').drop('km.value', 'km.unit')
    kcat_df = kcat_df.filter(
        pl.col('pmid').is_in(set(known_kcat_df['pmid']))
    )
    km_df = km_df.filter(
        pl.col('pmid').is_in(set(known_km_df['pmid']))
    )
    # dfs, metrics = asof_precision_recall(kcat_df, known_kcat_df, on='kcat.true_value', tolerance=1E-6, keep_all_columns=True)
    kcat_dfs, kcat_metrics = exact_precision_recall(kcat_df, known_kcat_df, on='kcat.true_value', tolerance=1E-6, keep_all_columns=True)

    # print(metrics)

    km_dfs, km_metrics = exact_precision_recall(km_df, known_km_df, on='km.true_value', tolerance=1E-6, keep_all_columns=True)
    return kcat_dfs, kcat_metrics, km_dfs, km_metrics
    # step 2b: match with brenda
    # _no_scientific_notation
    # if not os.path.exists(match_dest):
    # gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)


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