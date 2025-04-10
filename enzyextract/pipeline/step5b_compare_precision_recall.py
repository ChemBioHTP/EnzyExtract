import os
import re

from Bio.Data.IUPACData import protein_letters_3to1_extended
from sklearn.metrics import mean_absolute_error, mean_squared_error

from enzyextract.metrics.precision_recall import exact_precision_recall, extract_value_and_unit_df
from enzyextract.pipeline.step5_compare_dfs import _remove_bad_es_calc_kcat_value_and_clean_mutants, load_runeem_df
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

    kcat_df = extract_value_and_unit_df(gpt_df.select('pmid', 'kcat'), 'kcat')
    km_df = extract_value_and_unit_df(gpt_df.select('pmid', 'km'), 'km')
    known_kcat_df = extract_value_and_unit_df(known_df.select('pmid', 'kcat'), 'kcat')
    known_km_df = extract_value_and_unit_df(known_df.select('pmid', 'km'), 'km')
    kcat_df = kcat_df.filter(
        pl.col('pmid').is_in(set(known_kcat_df['pmid']))
    )
    km_df = km_df.filter(
        pl.col('pmid').is_in(set(known_km_df['pmid']))
    )
    matched_view, TP, FP, FN = exact_precision_recall(kcat_df, known_kcat_df, col='kcat.true_value', tolerance=0.05)


    return matched_view
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
    # '_debug/cache/beluga_matched_based_on_EnzymeSubstrate.parquet'
    if working == 'beluga':
        gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)
    elif working == 'bucket':
        gpt_df = pl.read_parquet('data/valid/_valid_bucket-rebuilt.parquet')
    elif working == 'apogee':
        # gpt_df = pl.read_parquet('data/_compiled/apogee_all.parquet')
        gpt_df = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
    elif working == 'apatch':
        gpt_df = pl.read_parquet('data/valid/_valid_apatch-rebuilt.parquet')
    elif working == 'sabiork':
        gpt_df = pl.read_parquet('data/sabiork/valid_sabiork.parquet')
    elif working == 'everything':
        gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet')
    elif working == 'thedata':
        gpt_df = pl.read_parquet('data/export/TheData_kcat.parquet')
    else:
        raise ValueError("Invalid working")

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