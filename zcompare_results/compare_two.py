
import json
import os
import pandas as pd
import polars as pl
from enzyextract.backform.quality_assure import quality_assure_ai_message
from enzyextract.metrics.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score, get_perfects_only
from enzyextract.backform.process_human_perfect import form_human_perfect
from enzyextract.hungarian.csv_fix import clean_columns_for_valid, widen_df
from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.hungarian.postmatched_utils import convenience_rearrange_cols
from enzyextract.submit.batch_utils import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_file

from enzyextract.metrics.tupled_matching import compare_a_b
from enzyextract.metrics.quick_reports import print_stats, report_precision_recall
from enzyextract.metrics.es_metrics import compute_string_similarities

def script_compare_tupled():
    blacklist = whitelist = None
    redo = True

    brenda_csv = 'C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'
    brenda_df = clean_columns_for_valid(pd.read_csv(brenda_csv, dtype={'pmid': str}))
    brenda_df = widen_df(brenda_df)

    runeem_df = pd.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', dtype={'pmid': str})
    runeem_df = clean_columns_for_valid(runeem_df)

    # valid_a_df = brenda_df
    # valid_b_df = runeem_df

    valid_a_df = runeem_df
    valid_b_df = brenda_df

    whitelist = set(valid_b_df['pmid']) & set(valid_a_df['pmid'])

    write_dest = f"data/humaneval/comparisons/tupled_runeem_vs_brenda.csv"
    if os.path.exists(write_dest) and not redo:
        print("Already exists")
    else:
        stats, matched_df = compare_a_b(valid_a_df, valid_b_df, 'runeem_vs_brenda',
            blacklist=blacklist, whitelist=whitelist)
        print_stats(stats)
        matched_df.to_csv(write_dest, index=False)
    

        # convert to polars
    
    df = pl.read_csv(write_dest, 
                     schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 
                                       'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8})
    
    report_precision_recall(df)

    to_ec_df = pl.read_parquet('data/brenda/brenda_to_ec.parquet')
    df = compute_string_similarities(df, to_ec_df)

    has_ec2 = 'ec_2' in df.columns and df['ec_2'].drop_nulls().n_unique() > 0

    

    wanted = [
        'pmid', 
        'enzyme_or_ec_match', 
        'viable_ecs', 
        'viable_ecs_2' if 'viable_ecs_2' in df.columns else None,
        'ec_2' if has_ec2 else None,
        'ec_match', 'enzyme', 'enzyme_full' if 'enzyme_full' in df.columns else None, 
        'enzyme_preferred', 'enzyme_2', 'enzyme_match', 
        'substrate', 'substrate_full' if 'substrate_full' in df.columns else None,
        'substrate_preferred', 'substrate_2', 'substrate_match'
    ]
    wanted = [w for w in wanted if w is not None]
    view = df.select(wanted)

    df.write_parquet(f'data/humaneval/comparisons/rich/rich_brenda_runeem.parquet')
    view.write_parquet(f'data/humaneval/comparisons/rich/view_brenda_runeem.parquet')

    

def script_compare_untupled():
    pass

if __name__ == "__main__":
    script_compare_tupled()
    # script

    