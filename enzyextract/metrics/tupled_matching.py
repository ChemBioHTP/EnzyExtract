from enzyextract.hungarian.hungarian_matching import match_dfs_by_pmid
from enzyextract.hungarian.postmatched_utils import convenience_rearrange_cols
from enzyextract.metrics.get_perfects import count_enzyme_substrate_all_matched, get_agreement_score


def compare_a_b(
        valid_a_df, 
        valid_b_df,
        namespace, 
        *,
        version = None, 
        blacklist = None,
        whitelist = None, 
        coeffs=None
):
    """
    Computes the hungarian matching between valid_a_df and valid_b_df, and collects a few statistics.

    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    # monitor a batch and how many pass every single requirement
    # brenwi-giveboth-tuneboth, rekcat-giveboth-4o
    
    # if None, will re-match with ground-truth
    # matched_csv = f'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    
    # valids = []
    # valid_pmids = set()
    # total_ingested = 0
    
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
    if coeffs is None:
        coeffs = {
            # ignore descriptor
                "kcat": 0.5,
                "km": 1,
                "substrate": 0.01,
                "mutant": 2, # mutants are rare but probably reliable
            }
    # cache the ground truth

    matched_df = match_dfs_by_pmid(valid_a_df, valid_b_df, sorted(both_pmids), coefficients=coeffs)
    matched_df = convenience_rearrange_cols(matched_df)

    # unique_a_only = valid_a_df[~valid_a_df['pmid'].isin(both_pmids)]
    # unique_b_only = valid_b_df[~valid_b_df['pmid'].isin(both_pmids)]

    # matched_df = pd.concat([matched_df, unique_a_only, unique_b_only], ignore_index=True)

    agreement, total = get_agreement_score(matched_df, allow_brenda_missing=False)
    stats['num_rows_agree'] = agreement
    stats['num_rows_total'] = total
    ### PRINT
    print(f"Ground-truth Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")
    
    return stats, matched_df
    # you know what, print the type of each column because there is a bug
    # print(matched_df.dtypes)