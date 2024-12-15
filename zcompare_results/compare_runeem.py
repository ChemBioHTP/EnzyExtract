
import json
import os
import pandas as pd
import polars as pl
from enzyextract.backform.quality_assure import quality_assure_ai_message
from enzyextract.metrics.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score, get_perfects_only
from enzyextract.backform.process_human_perfect import form_human_perfect
from enzyextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.hungarian.postmatched_utils import convenience_rearrange_cols
from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_file

from enzyextract.metrics.tupled_matching import compare_a_b
from enzyextract.metrics.quick_reports import print_stats, report_precision_recall
from enzyextract.metrics.es_metrics import compute_string_similarities

    # print("Of that, this number is ready for fine-tune:", stats.get('fine_tune_ready', 'NA'))

def script1():
    # take from checkpoint, then compare against brenda
    coeffs ={
        # ignore descriptor
            "kcat": 0.8,
            "km": 0.5,
            "substrate": 0.2,
            "mutant": 1, # mutants are rare but probably reliable
        }
    
    brenda_csv = 'C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'
    brenda_df = prep_for_hungarian(pd.read_csv(brenda_csv))
    brenda_df = widen_df(brenda_df)
    
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/brenwi_runeem.csv' 
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv'
    ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv'
    ground_df = pd.read_csv(ground_truth_csv)
    ground_df = form_human_perfect(ground_df)
    
    write_dest = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/K63_vs_brenda.csv'

    necessary_pmids = set(ground_df['pmid'])
    # if necessary_pmids:
    matched_df = match_dfs_by_pmid(ground_df, brenda_df, sorted(necessary_pmids), coefficients=coeffs)
    matched_df = convenience_rearrange_cols(matched_df)
    
    agreement, total = get_agreement_score(matched_df)
    print(f"BRENDA Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
    
    

    matched_df.to_csv(write_dest, index=False)

if __name__ == "__main__":
    blacklist = whitelist = None
    redo = False
    # blacklist = pmids_from_cache("finetunes/pmids-rekcat-giveboth-train_1.train")
    # blacklist = pmids_from_cache("finetunes/tableless-oneshot.train")
    
    # whitelist = pmids_from_batch("C:/conjunct/table_eval/batches/enzy/brenda-rekcat-md-v1-2_1.jsonl")
    # whitelist = pmids_from_batch("C:/conjunct/table_eval/batches/enzy/tableless-oneshot_1.jsonl")

    
    # namespace = 'brenda-rekcat-md-v1-2' 
    # namespace = 'brenda-rekcat-tuneboth' # rekcat-giveboth-4o
    # namespace = 'brenda-asm-apogee-4o'
    # namespace = 'humaneval-apogee-20241029'
    namespace = 'beluga-t2neboth'
    structured = namespace.endswith('-str')
    version = None
    # compl_folder = 'C:/conjunct/table_eval/completions/enzy'
    compl_folder = 'completions/enzy'

    # valid_a_df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')
    # valid_b_df = pd.read_csv('data/humaneval/runeem components/apogee_runeem_20241025.csv', dtype={'pmid': str})
    
    valid_a_df = pd.read_csv(f'data/valid/_valid_{namespace}_1.csv', dtype={'pmid': str})
    valid_b_df = pd.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', dtype={'pmid': str})

    whitelist = set(valid_b_df['pmid'])

    # write_dest = f"data/humaneval/comparisons/compare_{namespace}_runeem.csv"
    write_dest = f"data/humaneval/comparisons/tupled/tupled_{namespace}_runeem.csv"
    print("Namespace:", namespace)
    if os.path.exists(write_dest) and not redo:
        print("Already exists")
    else:
        stats, matched_df = compare_a_b(valid_a_df, valid_b_df, 
            namespace=namespace, version=version, blacklist=blacklist, whitelist=whitelist)
        print_stats(stats)
        matched_df.to_csv(write_dest, index=False)
    
        # append stats
        # dest = 'data/stats/compare_a_b_stats.tsv'
        # df = pd.DataFrame([stats])

        # if not os.path.exists(dest):
        #     df.to_csv(dest, index=False, sep='\t')
        # else:
        #     df.to_csv(dest, mode='a', header=False, index=False, sep='\t')
    

        # convert to polars
    df = pl.read_csv(write_dest, 
                     schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 
                                       'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8})
    
    report_precision_recall(df)

    to_ec_df = pl.read_parquet('data/brenda/brenda_to_ec.parquet')


    df = compute_string_similarities(df, to_ec_df)

    # add in gpt similarity, if it exists
    if os.path.exists(f'data/synonyms/es/{namespace}-essim-runeem-4ostruct_1.parquet'):
        gpt_df = pl.read_parquet(f'data/synonyms/es/{namespace}-essim-runeem-4ostruct_1.parquet')
        
        gpt_df_enzyme = gpt_df.filter(~pl.col('is_substrate')).select([
            'a_input', 'b_input', 'confidence', 'are_equivalent'
        ]).rename({'confidence': 'gpt_enzyme_confidence', 'are_equivalent': 'gpt_enzyme_equivalent'})
        gpt_df_substrate = gpt_df.filter(pl.col('is_substrate')).select([
            'a_input', 'b_input', 'confidence', 'are_equivalent'
        ]).rename({'confidence': 'gpt_substrate_confidence', 'are_equivalent': 'gpt_substrate_equivalent'})

        from enzyextract.thesaurus.convert_es import confer_es_preferred
        df = confer_es_preferred(df )
        df = df.join(gpt_df_enzyme, left_on=['enzyme_preferred', 'enzyme_preferred_2'], 
                right_on=['a_input', 'b_input'], how='left')
        df = df.join(gpt_df_substrate, left_on=['substrate_preferred', 'substrate_preferred_2'], 
                right_on=['a_input', 'b_input'], how='left')
    

    view = df.select([
        'pmid', 'enzyme_or_ec_match', 'viable_ecs', 'viable_ecs_2', 'ec_match', 
        'enzyme', 'enzyme_full', 'enzyme_preferred', 'enzyme_2', 'enzyme_match', 
        'substrate', 'substrate_full', 'substrate_preferred', 'substrate_2', 'substrate_match'
    ])

    wanted = [
        'pmid', 
        'enzyme_or_ec_match', 
        'viable_ecs', 
        'viable_ecs_2' if 'viable_ecs_2' in df.columns else None,
        # 'ec_2' if has_ec2 else None,
        'ec_match', 'enzyme', 'enzyme_full' if 'enzyme_full' in df.columns else None, 
        'enzyme_preferred', 'enzyme_2', 'enzyme_match', 
        'gpt_enzyme_confidence', 'gpt_enzyme_equivalent',
        'substrate', 'substrate_full' if 'substrate_full' in df.columns else None,
        'substrate_preferred', 'substrate_2', 'substrate_match',
        'gpt_substrate_confidence', 'gpt_substrate_equivalent'

    ]
    wanted = [w for w in wanted if w is not None]
    view = df.select(wanted)
    # 'enzyme_similarity', 

    df.write_parquet(f'data/humaneval/comparisons/rich/rich_{namespace}_runeem.parquet')
    view.write_parquet(f'data/humaneval/comparisons/rich/view_{namespace}_runeem.parquet')
    
    
