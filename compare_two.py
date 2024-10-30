
import json
import os
import pandas as pd
from kcatextract.backform.quality_assure import quality_assure_ai_message
from kcatextract.backform.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score, get_perfects_only
from kcatextract.backform.process_human_perfect import form_human_perfect
from kcatextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from kcatextract.hungarian.hungarian_matching import match_dfs_by_pmid
from kcatextract.hungarian.postmatched_utils import convenience_rearrange_cols
from kcatextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from kcatextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from kcatextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_file

def print_stats(stats):
    # pretty print stats
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    # print("Of that, this number is ready for fine-tune:", stats.get('fine_tune_ready', 'NA'))


def compare_a_b(
        valid_a_df, 
        valid_b_df,
        namespace, 
        *,
        version = None, 
        blacklist = None,
        whitelist = None
):
    """
    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    # monitor a batch and how many pass every single requirement
    
    # brenwi-giveboth-tuneboth, rekcat-giveboth-4o
    
    # if None, will re-match with ground-truth
    # matched_csv = f'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    
    valids = []
    valid_pmids = set()
    total_ingested = 0
    
    stats = {}
    stats['namespace'] = namespace
    print("Namespace:", namespace)
    valid_a_df = valid_a_df.astype({'pmid': 'str'})
    valid_b_df = valid_b_df.astype({'pmid': 'str'})

    # apply an early whitelist/blacklist

    if blacklist is None:
        blacklist = set()
    if whitelist is None:
        valid_a_df = valid_a_df[~valid_a_df['pmid'].isin(blacklist)]
        valid_b_df = valid_b_df[~valid_b_df['pmid'].isin(blacklist)]
    else:
        valid_a_df = valid_a_df[valid_a_df['pmid'].isin(whitelist - blacklist)]
        valid_b_df = valid_b_df[valid_b_df['pmid'].isin(whitelist - blacklist)]

    a_pmids = set(valid_a_df['pmid'])
    stats['pmid_count_a'] = len(a_pmids)
    b_pmids = set(valid_b_df['pmid'])
    stats['pmid_count_b'] = len(b_pmids)

    both_pmids = a_pmids & b_pmids
    stats['pmid_count_both'] = len(both_pmids)

    a_eas = count_enzyme_substrate_all_matched(valid_a_df, how='rows')
    b_eas = count_enzyme_substrate_all_matched(valid_b_df, how='rows')
    stats['enzyme_and_substrate_a'] = len(a_eas)
    stats['enzyme_and_substrate_b'] = len(b_eas)
    
    # count number of valid km
    stats['num_km_a'] = len(valid_a_df['km'].dropna())
    stats['num_km_b'] = len(valid_b_df['km'].dropna())
    stats['num_kcat_a'] = len(valid_a_df['kcat'].dropna())
    stats['num_kcat_b'] = len(valid_b_df['kcat'].dropna())
    
    # Match 1: now, calculate agreement score with grount-truth
    coeffs ={
        # ignore descriptor
            "kcat": 0.5,
            "km": 1,
            "substrate": 0.01,
            "mutant": 2, # mutants are rare but probably reliable
        }
    # cache the ground truth

    matched_df = match_dfs_by_pmid(valid_a_df, valid_b_df, both_pmids, coefficients=coeffs)
    matched_df = convenience_rearrange_cols(matched_df)

    unique_a_only = valid_a_df[~valid_a_df['pmid'].isin(both_pmids)]
    unique_b_only = valid_b_df[~valid_b_df['pmid'].isin(both_pmids)]

    matched_df = pd.concat([matched_df, unique_a_only, unique_b_only], ignore_index=True)

    agreement, total = get_agreement_score(matched_df, allow_brenda_missing=False)
    stats['num_rows_agree'] = agreement
    stats['num_rows_total'] = total
    ### PRINT
    print(f"Ground-truth Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
    
        
    
    
    print_stats(stats)
    
    return stats, matched_df
    # you know what, print the type of each column because there is a bug
    # print(matched_df.dtypes)

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
    # blacklist = pmids_from_cache("finetunes/pmids-rekcat-giveboth-train_1.train")
    # blacklist = pmids_from_cache("finetunes/tableless-oneshot.train")
    
    # whitelist = pmids_from_batch("C:/conjunct/table_eval/batches/enzy/brenda-rekcat-md-v1-2_1.jsonl")
    # whitelist = pmids_from_batch("C:/conjunct/table_eval/batches/enzy/tableless-oneshot_1.jsonl")

    
    # namespace = 'brenda-rekcat-md-v1-2' 
    # namespace = 'brenda-rekcat-tuneboth' # rekcat-giveboth-4o
    # namespace = 'brenda-asm-apogee-4o'
    namespace = 'humaneval-apogee-20241029'
    structured = namespace.endswith('-str')
    version = None
    # compl_folder = 'C:/conjunct/table_eval/completions/enzy'
    compl_folder = 'completions/enzy'

    valid_a_df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')
    valid_b_df = pd.read_csv('data/humaneval/apogee_runeem_20241025.csv')
    valid_b_df = valid_b_df.astype({'pmid': 'str'})
    whitelist = set(valid_b_df['pmid'])

    stats, matched_df = compare_a_b(valid_a_df, valid_b_df, 
        namespace=namespace, version=version, blacklist=blacklist, whitelist=whitelist)
    matched_df.to_csv(f"data/humaneval/compare_{namespace}_{version}.csv", index=False)
    
    # append stats
    dest = 'data/stats/compare_a_b_stats.tsv'
    df = pd.DataFrame([stats])

    if not os.path.exists(dest):
        df.to_csv(dest, index=False, sep='\t')
    else:
        df.to_csv(dest, mode='a', header=False, index=False, sep='\t')
        
    
    
