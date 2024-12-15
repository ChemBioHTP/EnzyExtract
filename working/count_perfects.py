
import os
import pandas as pd
from enzyextract.backform.quality_assure import quality_assure_ai_message
from enzyextract.metrics.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score, get_perfects_only
from enzyextract.backform.process_human_perfect import form_human_perfect
from enzyextract.hungarian.csv_fix import prep_for_hungarian, widen_df
from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.hungarian.postmatched_utils import convenience_rearrange_cols
from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_file
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df


def script0():
    # monitor a batch and how many pass every single requirement
    compl_folder = 'completions/enzy'
    # brenwi-giveboth-tuneboth, rekcat-giveboth-4o
    namespace = 'brenwi-giveboth-tuned' # tuned # tableless-oneshot # brenda-rekcat-md-v1-2
    version = None # None
    
    
    brenda_csv = 'C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'
    
    ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/brenwi_runeem.csv' 
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv'
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv'

    
    # if None, will re-match with ground-truth
    # matched_csv = f'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    
    filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
    
    matched_csv = f'_debug/_cache_vbrenda/_cache_{namespace}_{version}.csv' # 'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    _valid_csv = f'_debug/_cache_valid/_valid_{namespace}_{version}.csv'
    
    valids = []
    valid_pmids = set()
    totals = 0
    
    print("Namespace:", namespace)
    # if valid_csv exists, we can save some time
    if _valid_csv and os.path.exists(_valid_csv):
        valid_df = pd.read_csv(_valid_csv)
        valid_df = valid_df.astype({'pmid': 'str'})
        valid_pmids = set(valid_df['pmid'])
        print("Using existing valid csv.")
        
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            if finish_reason != 'length':
                totals += 1
        print("Started with", totals, "PMIDs")
        print("Valid PMIDs:", len(valid_pmids))
    else:
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            pmid = pmid_from_usual_cid(custom_id)
            
            content = content.replace('\nextras:\n', '\ndata:\n') # blunder
            if finish_reason == 'length':
                print("Too long:", pmid)
                continue
            totals += 1
            for _, yaml in fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid)): # 
                # assert _ == None
                
                df, context = yaml_to_df(yaml, auto_context=True, debugpmid=None) # pmid, silence debug
                df['pmid'] = pmid
                if df.empty:
                    continue
                valids.append(df)
                valid_pmids.add(str(pmid)) # needs to be a set

        print("Started with", totals, "PMIDs")
        print("Valid PMIDs:", len(valid_pmids))
        
        valid_df = prep_for_hungarian(pd.concat(valids)) # bad units for km and kcat are rejected here
        valid_df = valid_df.astype({'pmid': 'str'})
        # no longer necessary since pmid is str
        
        
        if _valid_csv and not os.path.exists(_valid_csv):
            print("Writing to", _valid_csv)
            valid_df.to_csv(_valid_csv, index=False)
    
    # pmids_all_matched = count_enzyme_substrate_all_matched(valid_df, how='pmid')
    # print("Of that, this many PMID have all enzyme and substrate matched:", len(pmids_all_matched))
    valid_km = valid_df[valid_df['km'].notnull()]
    valid_km_enzyme_substrate = count_enzyme_substrate_all_matched(valid_km, how='rows')
    print(f"By the way, ={len(valid_km_enzyme_substrate)}/{len(valid_km)} Km have both enzyme and substrate")
    
    _have_both_enzyme_substrate = set(count_enzyme_substrate_all_matched(valid_km, how='pmid'))
    # print(f"These pmids don't: {valid_pmids - _have_both_enzyme_substrate}")
    # valid_pmids = _at_least_one_empty
    
    # we can exclude pdfs now
    blacklist = pmids_from_cache("finetunes/tableless-oneshot.train")
    valid_pmids -= blacklist
    _have_both_enzyme_substrate -= blacklist
    # whitelist = pmids_from_batch("batches/enzy/brenda-rekcat-md-v1-2_1.jsonl")
    # valid_pmids = valid_pmids & whitelist
    # print("Whitelisted PMIDs:", len(valid_pmids))
    
    # Match 1: now, calculate agreement score with grount-truth
    coeffs ={
        # ignore descriptor
            "kcat": 0.8,
            "km": 0.5,
            "substrate": 0.2,
            "mutant": 1, # mutants are rare but probably reliable
        }
    if ground_truth_csv and os.path.exists(ground_truth_csv):
        ground_df = pd.read_csv(ground_truth_csv)
        ground_df = form_human_perfect(ground_df)
        
        necessary_pmids = valid_pmids & set(ground_df['pmid'])
        if necessary_pmids:
            gt_matched_df = match_dfs_by_pmid(valid_df, ground_df, sorted(necessary_pmids), coefficients=coeffs)
            gt_matched_df = convenience_rearrange_cols(gt_matched_df)
            agreement, total = get_agreement_score(gt_matched_df, allow_brenda_missing=True)
            print(f"Ground-truth Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
            gt_matched_df.to_csv(f"_debug/_cache_vk63/_vs_gt_{namespace}_{version}.csv", index=False)
        
    
    
    # Match2: need to match with BRENDA
    # stage 2: hungarian matching
    
    if matched_csv and os.path.exists(matched_csv):
        print("Using existing matched csv.")
        matched_df = pd.read_csv(matched_csv)
        matched_df = matched_df.astype({'pmid': 'str'})
        matched_df = matched_df[matched_df['pmid'].isin(valid_pmids)]
    else:
        
        
        brenda_df = prep_for_hungarian(pd.read_csv(brenda_csv))
        brenda_df = widen_df(brenda_df)
        
        # ground_truth_df = ground_truth_df.astype({'pmid': 'str'})
        matched_df = match_dfs_by_pmid(valid_df, brenda_df, sorted(valid_pmids), coeffs)
        
        matched_df = convenience_rearrange_cols(matched_df)
        
        if matched_csv and not os.path.exists(matched_csv):
            matched_df.to_csv(matched_csv, index=False)
    
    # kcat and km versus brenda
    matched_df.replace('', pd.NA, inplace=True)
    print(f"Cardinality kcat vs BRENDA: ={len(matched_df['kcat'].dropna())}/{len(matched_df['kcat_2'].dropna())}")
    print(f"Cardinality km vs BRENDA: ={len(matched_df['km'].dropna())}/{len(matched_df['km_2'].dropna())}")
    
    # get agreement score
    agreement, total = get_agreement_score(matched_df)
    print(f"BRENDA Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
    
    superset_df = get_perfects_only(matched_df, allow_superset=True)
    superset_pmids = superset_df['pmid'].unique()
    print("Of all pmids, this many are superset of BRENDA:", len(superset_pmids), 
            f"({len(superset_df['kcat'].dropna())} kcat, {len(superset_df['km'].dropna())} km)")
    
    perfect_df = get_perfects_only(matched_df, allow_superset=False)
    
    perfect_pmids = set(perfect_df['pmid'])
    print("Of all pmids, this many perfectly match BRENDA:", len(perfect_pmids), 
          f"({len(perfect_df['kcat'].dropna())} kcat, {len(perfect_df['km'].dropna())} km)")

    
        
    # see how many also pass the QA process for backform
    backform_pmids = []
    for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
        pmid = pmid_from_usual_cid(custom_id)
        if str(pmid) in (perfect_pmids & _have_both_enzyme_substrate):
            problems, fixed = quality_assure_ai_message(content)
            if not problems:
                backform_pmids.append(pmid)
    print("Of that, this number is ready for fine-tune:", len(backform_pmids))


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
    script0()
        
    
    
