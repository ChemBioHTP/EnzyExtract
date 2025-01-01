
import json
import os
import pandas as pd
from enzyextract.backform.quality_assure import quality_assure_ai_message
from enzyextract.metrics.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score, get_perfects_only
from enzyextract.backform.process_human_perfect import form_human_perfect
from enzyextract.hungarian.csv_fix import clean_columns_for_valid, widen_df
from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.hungarian.postmatched_utils import convenience_rearrange_cols
from enzyextract.submit.batch_utils import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_file


# brenwi-giveboth-tuneboth_2
def print_stats(stats):
    print('\n')
    if 'gt_agreement' in stats:
        print(f"Ground-truth Agreement score: ={stats['gt_agreement']}/{stats['gt_total']} which is ({stats['gt_agreement']/stats['gt_total']:.4f})")
    
    print(f"Km with Enzyme And Substrate: ={stats['valid_km_with_eands']}/{stats['valid_km']} which is ({stats['percent_km_with_eands']:.4f})")
    
    print(f"Cardinality kcat vs BRENDA: ={stats['our_kcat']}/{stats['brenda_kcat']}")
    print(f"Cardinality km vs BRENDA: ={stats['our_km']}/{stats['brenda_km']}")
    
    if stats.get('brenda_total'):
        print(f"BRENDA Agreement score: ={stats['brenda_agreement']}/{stats['brenda_total']} which is ({stats['brenda_agreement']/stats['brenda_total']:.4f})")
        
        print(f"BRENDA-superset (pmid, kcat, km): {stats['brenda_superset']}\t{stats['brenda_superset_kcat']}\t{stats['brenda_superset_km']}")
        print(f"BRENDA-perfect (pmid, kcat, km): {stats['brenda_perfect']}\t{stats['brenda_perfect_kcat']}\t{stats['brenda_perfect_km']}")
    else:
        print("No BRENDA data.")
    
    # print("Of that, this number is ready for fine-tune:", stats.get('fine_tune_ready', 'NA'))


def generate_bmatched_csv(namespace = 'rekcat-giveboth-4o', 
        version = None, 
        compl_folder = 'completions/enzy',
        use_yaml=True,
        silence=True):
    """
    Given a namespace, generate the bmatched csv
    """
    
    brenda_csv = 'C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'
    
    
    filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
    print(f"Located {filename} version {version} in {compl_folder}")
    
    matched_csv = f'data/matched/with_brenda/_cache_{namespace}_{version}.csv' # 'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    _valid_csv = f'data/valid/_valid_{namespace}_{version}.csv'
    
    valids = []
    valid_pmids = set()
    total_ingested = 0
    
    print("Namespace:", namespace)
    # if valid_csv exists, we can save some time
    if _valid_csv and os.path.exists(_valid_csv):
        valid_df = pd.read_csv(_valid_csv, dtype={'pmid': 'str'})
        valid_pmids = set(valid_df['pmid'])
        print("Using existing valid csv.")
        
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            if finish_reason != 'length':
                total_ingested += 1
    else:
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            pmid = str(pmid_from_usual_cid(custom_id))
            
            content = content.replace('\nextras:\n', '\ndata:\n') # blunder
            if finish_reason == 'length':
                print("Too long:", pmid)
                continue
            total_ingested += 1
            
            if use_yaml:
                _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid))
            else:
                # assume json content
                _generator = [(0, equivalent_from_json_schema(content))]
            for _, yaml in _generator: # 
                # assert _ == None
                
                df, context = yaml_to_df(yaml, auto_context=True, debugpmid=None if silence else pmid) # pmid, silence debug
                df['pmid'] = pmid
                if df.empty:
                    continue
                valids.append(df)
                valid_pmids.add(pmid) # needs to be a set
        
        valid_df = clean_columns_for_valid(pd.concat(valids)) # bad units for km and kcat are rejected here
        # valid_df = valid_df.astype({'pmid': 'str'})
        
        if _valid_csv and not os.path.exists(_valid_csv):
            print("Writing to", _valid_csv)
            valid_df.to_csv(_valid_csv, index=False)
    
    # Match 1: now, calculate agreement score with grount-truth
    coeffs ={
        # ignore descriptor
            "kcat": 0.5,
            "km": 1,
            "substrate": 0.01,
            "mutant": 2, # mutants are rare but probably reliable
        }
    
    # Match2: need to match with BRENDA
    # stage 2: hungarian matching
        
    brenda_df = clean_columns_for_valid(pd.read_csv(brenda_csv))
    brenda_df = widen_df(brenda_df)
    
    # ground_truth_df = ground_truth_df.astype({'pmid': 'str'})
    matched_df = match_dfs_by_pmid(valid_df, brenda_df, sorted(valid_pmids), coeffs)
    
    matched_df = convenience_rearrange_cols(matched_df)
    
    if matched_csv and not os.path.exists(matched_csv):
        matched_df.to_csv(matched_csv, index=False)
        

def run_stats(*, 
        namespace = 'rekcat-giveboth-4o', # tuned # tableless-oneshot # brenda-rekcat-md-v1-2
        version = None, # None
        compl_folder = 'completions/enzy', # C:/conjunct/table_eval/completions/enzy
        blacklist = None,
        whitelist = None,
        against_brenda=True,
        silence = True,
        use_yaml=True
):
    """
    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    # monitor a batch and how many pass every single requirement
    
    # brenwi-giveboth-tuneboth, rekcat-giveboth-4o
    
    
    brenda_csv = 'C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv'
    
    ground_truth_csv = None # 'C:/conjunct/vandy/yang/corpora/eval/brenwi_runeem.csv' 
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat_checkpoint_v4_only_values_checked.csv'
    # ground_truth_csv = 'C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv'

    
    # if None, will re-match with ground-truth
    # matched_csv = f'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    
    filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
    print(f"Located {filename} version {version} in {compl_folder}")
    
    matched_csv = f'data/matched/with_brenda/_cache_{namespace}_{version}.csv' # 'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    _valid_csv = f'data/valid/_valid_{namespace}_{version}.csv'
    
    valids = []
    valid_pmids = set()
    total_ingested = 0
    
    stats = {}
    stats['namespace'] = namespace
    print("Namespace:", namespace)
    # if valid_csv exists, we can save some time
    if _valid_csv and os.path.exists(_valid_csv):
        valid_df_cached = pd.read_csv(_valid_csv)
        valid_df_cached = valid_df_cached.astype({'pmid': 'str'})
        # valid_df = valid_df.astype(str).replace('nan', pd.NA)
        # print(valid_df_cached.dtypes)
        valid_pmids = set(valid_df_cached['pmid'])
        print("Using existing valid csv.")
        
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            if finish_reason != 'length':
                total_ingested += 1
        stats['total_ingested'] = total_ingested
        stats['valid_pmids'] = len(valid_pmids)
        
        valid_df = valid_df_cached

    else:
    # if True:
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            pmid = str(pmid_from_usual_cid(custom_id))
            
            content = content.replace('\nextras:\n', '\ndata:\n') # blunder
            if finish_reason == 'length':
                print("Too long:", pmid)
                continue
            total_ingested += 1
            
            if use_yaml:
                _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid))
            else:
                # assume json content
                _generator = [(0, equivalent_from_json_schema(content))]
            for _, yaml in _generator: # 
                # assert _ == None
                
                df, context = yaml_to_df(yaml, auto_context=True, debugpmid=None if silence else pmid) # pmid, silence debug
                df['pmid'] = pmid
                if df.empty:
                    continue
                valids.append(df)
                valid_pmids.add(str(pmid)) # needs to be a set

        stats['total_ingested'] = total_ingested
        stats['valid_pmids'] = len(valid_pmids)
        
        valid_df = clean_columns_for_valid(pd.concat(valids)) # bad units for km and kcat are rejected here
        valid_df = valid_df.astype({'pmid': 'str'})
        # print(valid_df.dtypes)
        
        if _valid_csv and not os.path.exists(_valid_csv):
            print("Writing to", _valid_csv)
            valid_df.to_csv(_valid_csv, index=False)
        
        # valid_df = pd.read_csv(_valid_csv)
    
    # get rows where valid_df_cached['substrate'] is null but valid_df['substrate'] is notnull
    # difference_df = valid_df_cached[valid_df_cached['substrate'].isnull() & valid_df['substrate'].notnull()]
    
    print("Started with", stats['total_ingested'], "PMIDs")
    print("Valid PMIDs:", stats['valid_pmids'])
    
    # pmids_all_matched = count_enzyme_substrate_all_matched(valid_df, how='pmid')
    # print("Of that, this many PMID have all enzyme and substrate matched:", len(pmids_all_matched))
    valid_km = valid_df[valid_df['km'].notnull()]
    valid_km_enzyme_substrate = count_enzyme_substrate_all_matched(valid_km, how='rows')
    stats['valid_km_with_eands'] = len(valid_km_enzyme_substrate)
    stats['valid_km'] = len(valid_km)
    if not stats['valid_km']:
        print("No valid KM found.")
    else:
        stats['percent_km_with_eands'] = stats['valid_km_with_eands'] / stats['valid_km']
    
    _have_both_enzyme_substrate = set(count_enzyme_substrate_all_matched(valid_km, how='pmid'))
    # print(f"These pmids don't: {valid_pmids - _have_both_enzyme_substrate}")
    # valid_pmids = _at_least_one_empty
    
        
    
    # whitelist = pmids_from_batch("batches/enzy/brenda-rekcat-md-v1-2_1.jsonl")
    # valid_pmids = valid_pmids & whitelist
    # print("Whitelisted PMIDs:", len(valid_pmids))
    
    # Match 1: now, calculate agreement score with grount-truth
    coeffs ={
        # ignore descriptor
            "kcat": 0.5,
            "km": 1,
            "substrate": 0.01,
            "mutant": 2, # mutants are rare but probably reliable
        }
    # cache the ground truth
    if ground_truth_csv and os.path.exists(ground_truth_csv):
        ground_df = pd.read_csv(ground_truth_csv)
        ground_df = form_human_perfect(ground_df)
        
        necessary_pmids = valid_pmids & set(ground_df['pmid'])
        if necessary_pmids:
            # unfortunately, this is not deterministic
            # probably because of ties in hung matching
            gt_matched_df = match_dfs_by_pmid(valid_df, ground_df, sorted(necessary_pmids), coefficients=coeffs)
            gt_matched_df = convenience_rearrange_cols(gt_matched_df)
            agreement, total = get_agreement_score(gt_matched_df, allow_brenda_missing=True)
            stats['gt_agreement'] = agreement
            stats['gt_total'] = total
            ### PRINT
            # print(f"Ground-truth Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
            gt_matched_df.to_csv(f"data/_cache_vk63/_vs_gt_{namespace}_{version}.csv", index=False)
    
    # we should match everything, but a view can be only after the whitelist/blacklist
    # we can exclude pdfs now
    if whitelist is None:
        whitelist = valid_pmids
    if blacklist is None:
        blacklist = set()

    view_pmids = (valid_pmids & whitelist) - blacklist
    print("After whitelist/blacklist:", len(view_pmids))
    _have_both_enzyme_substrate = _have_both_enzyme_substrate & valid_pmids
        
    
    if not against_brenda:
        view_df = valid_df[valid_df['pmid'].isin(view_pmids)]

        stats['our_kcat'] = len(view_df['kcat'].dropna())
        stats['our_km'] = len(view_df['km'].dropna())
        
        stats['brenda_kcat'] = 0
        stats['brenda_km'] = 0
        
        stats['brenda_agreement'] = stats['brenda_total'] = \
            stats['brenda_superset'] = stats['brenda_superset_kcat'] = stats['brenda_superset_km'] = \
            stats['brenda_perfect'] = stats['brenda_perfect_kcat'] = stats['brenda_perfect_km'] = None
    else:
        # Match2: need to match with BRENDA
        # stage 2: hungarian matching
        if matched_csv and os.path.exists(matched_csv):
            print("Using existing matched csv.")
            matched_df = pd.read_csv(matched_csv)
            matched_df = matched_df.astype({'pmid': 'str'})
            matched_df = matched_df[matched_df['pmid'].isin(valid_pmids)]
        else:
            
            brenda_df = clean_columns_for_valid(pd.read_csv(brenda_csv))
            brenda_df = widen_df(brenda_df)
            
            # ground_truth_df = ground_truth_df.astype({'pmid': 'str'})
            matched_df = match_dfs_by_pmid(valid_df, brenda_df, sorted(valid_pmids), coeffs)
            
            matched_df = convenience_rearrange_cols(matched_df)
            
            if matched_csv and not os.path.exists(matched_csv):
                matched_df.to_csv(matched_csv, index=False)
        
        # kcat and km versus brenda
        matched_df.replace('', pd.NA, inplace=True)

        view_df = matched_df[matched_df['pmid'].isin(view_pmids)]

        stats['our_kcat'] = len(view_df['kcat'].dropna())
        stats['brenda_kcat'] = len(view_df['kcat_2'].dropna())
        
        stats['our_km'] = len(view_df['km'].dropna())
        stats['brenda_km'] = len(view_df['km_2'].dropna())
        ### PRINT
        
        # get agreement score
        agreement, total = get_agreement_score(view_df)
        stats['brenda_agreement'] = agreement
        stats['brenda_total'] = total
        ### PRINT
        
        superset_df = get_perfects_only(view_df, allow_superset=True)
        superset_pmids = superset_df['pmid'].unique()
        stats['brenda_superset'] = len(superset_pmids)
        stats['brenda_superset_kcat'] = len(superset_df['kcat'].dropna())
        stats['brenda_superset_km'] = len(superset_df['km'].dropna())
        
        ### PRINT
        
        
        perfect_df = get_perfects_only(view_df, allow_superset=False)
        
        perfect_pmids = set(perfect_df['pmid'])
        stats['brenda_perfect'] = len(perfect_pmids)
        stats['brenda_perfect_kcat'] = len(perfect_df['kcat'].dropna())
        stats['brenda_perfect_km'] = len(perfect_df['km'].dropna())
    ### PRINT

    
        
    # see how many also pass the QA process for backform
    # backform_pmids = []
    # for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
    #     pmid = pmid_from_usual_cid(custom_id)
    #     if str(pmid) in (perfect_pmids & _have_both_enzyme_substrate):
    #         problems, fixed = quality_assure_ai_message(content)
    #         if not problems:
    #             backform_pmids.append(pmid)
    # stats['fine_tune_ready'] = len(backform_pmids)
    ### PRINT
    
    print_stats(stats)
    
    return stats
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
    brenda_df = clean_columns_for_valid(pd.read_csv(brenda_csv))
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

    # read whitelist as the pmids from data/matched/with_brenda/_cache_openelse-brenda-xml-4o
    # whitelist_df = pd.read_csv('data/matched/with_brenda/_cache_openelse-brenda-xml-4o_1.csv', dtype={'pmid': str})
    # whitelist_df = pd.read_csv('data/matched/with_brenda/_cache_openelse-bucket-md-4o-str_1.csv', dtype={'pmid': str})
    # whitelist_df = pd.read_csv('data/matched/with_brenda/_cache_openelse-brenda-md-4o_1.csv', dtype={'pmid': str})
    # whitelist = set(whitelist_df['pmid'])
    # whitelist_df = pd.read_csv('data/matched/with_brenda/_cache_openelse-brenda-xml-4o_1.csv', dtype={'pmid': str})
    # whitelist &= set(whitelist_df['pmid'])
    
    # namespace = 'brenda-rekcat-md-v1-2' 
    # namespace = 'brenda-rekcat-tuneboth' # rekcat-giveboth-4o
    # namespace = 'brenda-asm-apogee-4o'
    # namespace = 'openelse-brenda-xml-4o'
    # namespace = 'openelse-bucket-md-4o-str'
    # namespace = 'openelse-brenda-md-4o'
    namespace = 'beluga-t2neboth'

    
    structured = namespace.endswith('-str')
    version = None
    # compl_folder = 'C:/conjunct/table_eval/completions/enzy'
    compl_folder = 'completions/enzy'

    # merge fragments
    # check to see if we need to merge
    need_merge = False
    filename, version = locate_correct_batch(compl_folder, namespace)
    if filename.endswith('0.jsonl'):
        need_merge = True
    
    if need_merge:
        from enzyextract.submit.openai_management import merge_chunked_completions
        print(f"Merging all chunked completions for {filename} v{version}. Confirm? (y/n)")    
        if input() != 'y':
            exit(0)
        merge_chunked_completions(namespace, version=version, compl_folder=compl_folder, dest_folder=compl_folder)

    stats = run_stats(namespace=namespace, version=version, compl_folder=compl_folder, blacklist=blacklist, whitelist=whitelist,
              against_brenda=True,
              silence=False,
              use_yaml=not structured)

        # convert to polars
    import polars as pl
    df = pl.read_csv(f'data/matched/with_brenda/_cache_{namespace}_1.csv', 
                     schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8})
    
    from enzyextract.metrics.quick_reports import report_precision_recall
    report_precision_recall(df)

    to_ec_df = pl.read_parquet('data/brenda/brenda_to_ec.parquet')

    from enzyextract.metrics.es_metrics import compute_string_similarities
    df = compute_string_similarities(df, to_ec_df)

    df.write_parquet(f'data/humaneval/comparisons/rich/rich_{namespace}_brenda.parquet')
        
    
    
