import polars as pl
from tqdm import tqdm
import re
from enzyextract.utils.pmid_management import pmids_from_cache
from enzyextract.hungarian.hungarian_matching import parse_value_and_unit

def load_brenda_df(pmids):
    df = pl.read_csv('data/_compiled/apogee-brenda-and-nonbrenda.tsv', separator='\t', 
                     schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8})
    # df = df.with_columns(pl.col('pmid').cast(pl.Utf8))
    df = df.filter(pl.col('pmid').is_in(pmids))
    return df

def each_brenda_value_matched(df_subset, care_about_kcat_km=False):
    match_filter = ['km', 'kcat']
    if care_about_kcat_km:
        match_filter.append('kcat_km')
    for key in match_filter:
        brenda_col = f'{key}_2'
        our_col = key
        mismatch = df_subset.filter(
            pl.col(brenda_col).is_not_null() & pl.col(our_col).is_null()
        )
        if mismatch.height > 0:
            return False
    return True

def each_brenda_value_nonnull(df_subset: pl.DataFrame, care_about_kcat_km=False):

    mismatch_filter = pl.col('km_2').is_null() & pl.col('kcat_2').is_null()
    if care_about_kcat_km:
        mismatch_filter &= pl.col('kcat_km_2').is_null()
    mismatch = df_subset.filter(
        # brenda is null, ours is not
        mismatch_filter
    )
    return mismatch.height == 0
    # mask = df_subset.select([
    #     pl.any([
    #         pl.col('km_2').is_not_null(),
    #         pl.col('kcat_2').is_not_null(),
    #         pl.col('kcat_km_2').is_not_null()
    #     ]).alias('any_not_null')
    # ])
    # return mask['any_not_null'].all()

_off_by_10_re = re.compile(r'^off by 10*$')
_wrong_unit_re = re.compile(r'^wrong unit$')

def get_pmids_by_category(df, pmids):
    perfect_pmids = []
    strict_superset_pmids = []
    off_by_unit_pmids = []
    matching_quirk_pmids = []
    strict_subset_pmids = []
    erroneous_pmids = []

    for pmid in tqdm(pmids):
        subset = df.filter(pl.col('pmid') == pmid)
        if subset.is_empty():
            continue
            
        km_feedback = subset['km_feedback'].drop_nulls().unique().to_list()
        kcat_feedback = subset['kcat_feedback'].drop_nulls().unique().to_list()

        if not each_brenda_value_matched(subset):
            # check for strict subset pmids
            if not km_feedback and not kcat_feedback and each_brenda_value_nonnull:
                # some brenda has is undetected by ours
                # but the good news is that the rest was able to be matched

                # turns out, sometimes brenda reports information in a way where kcat and km are on separate rows
                # which makes it difficult to match. 
                # we can detect this by checking the cardinality of reported unique kcat and km. If they are the same, 
                # then it could be that we actually have the same info, but it cannot be matched
                if subset['kcat_2'].n_unique() == subset['kcat'].n_unique() and subset['km_2'].n_unique() == subset['km'].n_unique():
                    matching_quirk_pmids.append(pmid)
                else:
                    strict_subset_pmids.append(pmid)
            else:
                erroneous_pmids.append(pmid)
            continue


        if not km_feedback and not kcat_feedback:
            if each_brenda_value_nonnull(subset):
                perfect_pmids.append(pmid)
            else:
                strict_superset_pmids.append(pmid)
        else:
            km_only_unit_wrong = all(_off_by_10_re.match(feedback) for feedback in km_feedback)
            kcat_only_unit_wrong = all(_off_by_10_re.match(feedback) or _wrong_unit_re.match(feedback) for feedback in kcat_feedback)

            if km_only_unit_wrong and kcat_only_unit_wrong:
                off_by_unit_pmids.append(pmid)

    return perfect_pmids, strict_superset_pmids, off_by_unit_pmids, strict_subset_pmids, erroneous_pmids, matching_quirk_pmids

def count_boring_new_kcat(df_1pmid):
    """
    Given a df (with only 1 pmid), determine if we actually have new, unseen kcat values. 

    A "boring" kcat is one which is already present in brenda, but now we just duplicate it.
    :param df_1pmid:
    """

    brenda_kcat = df_1pmid['kcat_2'].drop_nulls().unique().to_list()
    our_kcat = df_1pmid['kcat'].drop_nulls().unique().to_list()

    if len(brenda_kcat) == len(our_kcat):
        return 0
    
    # old_news_kcat = set()
    # # for each row in the df, 
    # # if kcat_2 is populated, then add kcat_1 to old_news_kcat
    # for row in df_1pmid:
    #     if row['kcat_2'] is not None:
    #         old_news_kcat.add(row['kcat'])
    
    # count = 0
    # for row in df_1pmid:
    #     if row['kcat'] in old_news_kcat and row['kcat_2'] is None:
    #         count += 1
    # return count

        # Find all kcat values where kcat_2 is not null
    old_news_kcat = df_1pmid.filter(pl.col('kcat_2').is_not_null())['kcat'].unique()
    
    # Count rows where kcat is in old_news_kcat AND kcat_2 is null
    count = df_1pmid.filter(
        pl.col('kcat').is_in(old_news_kcat) & 
        pl.col('kcat_2').is_null()
    ).height
    return count

from enzyextract.metrics.polaric import mean_log_relative_ratio, precision_recall, get_accuracy_score, string_similarity

    

def script_make_subsets():
    pmids = pmids_from_cache('brenda')
    
    print("All PMIDS in BRENDA:", len(pmids))
    df = load_brenda_df(pmids)
    
    
    
    
    print("PMIDS accounted:", len(df['pmid'].unique()))
    
    pmids = df['pmid'].unique().to_list()
    
    # drop rows where all of these are null:
    # km, kcat, kcat_km, km_2, kcat_2, kcat_km_2
    df = df.filter(
        pl.col('km').is_not_null() | pl.col('kcat').is_not_null() | # pl.col('kcat_km').is_not_null() |
        pl.col('km_2').is_not_null() | pl.col('kcat_2').is_not_null() # | pl.col('kcat_km_2').is_not_null()
    )

    perfect_pmids, strict_superset_pmids, off_by_unit_pmids, strict_subset_pmids, erroneous_pmids, matching_quirk_pmids \
        = get_pmids_by_category(df, pmids)
    
    
    print("Perfect PMIDs:", len(perfect_pmids))
    print("Strict Superset PMIDs:", len(strict_superset_pmids))
    print("Off by Unit PMIDs:", len(off_by_unit_pmids))
    print("Strict Subset PMIDs:", len(strict_subset_pmids))
    print("Erroneous PMIDs:", len(erroneous_pmids))
    print("Matching Quirk PMIDs:", len(matching_quirk_pmids))
    
    perfect_df = df.filter(pl.col('pmid').is_in(perfect_pmids))
    strict_superset_df = df.filter(pl.col('pmid').is_in(strict_superset_pmids))
    off_by_unit_df = df.filter(pl.col('pmid').is_in(off_by_unit_pmids))
    strict_subset_df = df.filter(pl.col('pmid').is_in(strict_subset_pmids))
    erroneous_df = df.filter(pl.col('pmid').is_in(erroneous_pmids))
    quirky_df = df.filter(pl.col('pmid').is_in(matching_quirk_pmids))
    
    # perfect_df_viewer = perfect_df.select(['pmid', 'km', 'km_2', 'kcat', 'kcat_2', 'kcat_km', 'kcat_km_2'])
    # strict_superset_df_viewer = strict_superset_df.select(['pmid', 'km', 'km_2', 'kcat', 'kcat_2', 'kcat_km', 'kcat_km_2'])
    # off_by_unit_df_viewer = off_by_unit_df.select(['pmid', 'km', 'km_2', 'kcat', 'kcat_2', 'kcat_km', 'kcat_km_2'])
    
    # print(len(df))

    perfect_df.write_csv('data/_compiled/compare/brenda-perfect-apogee.tsv', separator='\t')
    strict_superset_df.write_csv('data/_compiled/compare/brenda-strict-superset-apogee.tsv', separator='\t')
    off_by_unit_df.write_csv('data/_compiled/compare/brenda-off-by-unit-apogee.tsv', separator='\t')
    strict_subset_df.write_csv('data/_compiled/compare/brenda-strict-subset-apogee.tsv', separator='\t')
    erroneous_df.write_csv('data/_compiled/compare/brenda-erroneous-apogee.tsv', separator='\t')
    quirky_df.write_csv('data/_compiled/compare/brenda-quirky-apogee.tsv', separator='\t')
    exit(0)

def script_precision_recall():
    pmids = pmids_from_cache('brenda')
    
    print("All PMIDS in BRENDA:", len(pmids))
    df = load_brenda_df(pmids)

    print("PMIDS accounted:", len(df['pmid'].unique()))

    TP, FP, FN = precision_recall(df)
    print("TP:", TP.height)
    print("FP:", FP.height)
    print("FN:", FN.height)

    precision = TP.height / (TP.height + FP.height)
    recall = TP.height / (TP.height + FN.height)

    print("Precision:", precision)
    print("Recall:", recall)

    agree, total = get_accuracy_score(df, allow_brenda_missing=True)
    print(f"BRENDA agreement score: ={agree}/{total} which is ({agree/total:.2f})")


    # calculate percent error
    kcat_error = mean_log_relative_ratio(df, 'kcat')
    km_error = mean_log_relative_ratio(df, 'km')
    print("kcat, Mean orders of magnitude error (perfect is 0):", kcat_error)
    print("kM, Mean orders of magnitude error (perfect is 0):", km_error)

    enzyme_similarity = string_similarity(df, 'enzyme')
    print("Enzyme similarity:", enzyme_similarity)
    substrate_similarity = string_similarity(df, 'substrate')
    print("Substrate similarity:", substrate_similarity)
    exit(0)

if __name__ == '__main__':
    script_precision_recall()
    script_make_subsets()

    perfect_df = pl.read_csv('data/_compiled/compare/brenda-perfect-apogee.tsv', separator='\t')
    strict_superset_df = pl.read_csv('data/_compiled/compare/brenda-strict-superset-apogee.tsv', separator='\t')
    off_by_unit_df = pl.read_csv('data/_compiled/compare/brenda-off-by-unit-apogee.tsv', separator='\t')

    # count nonnull kcat, then count boring_new kcat
    # print("Strict superset nonnull kcat:", len(strict_superset_df['kcat'].drop_nulls()))
    # total_boring = 0
    # boring_pmids = set()
    # # need to split dataframe by pmid
    # for pmid in strict_superset_df['pmid'].unique().to_list():
    #     df_1pmid = strict_superset_df.filter(pl.col('pmid') == pmid)
    #     dboring = count_boring_new_kcat(df_1pmid)
    #     if dboring > 0:
    #         total_boring += dboring
    #         boring_pmids.add(pmid)
    # print("Strict Superset boring new kcat count:", total_boring)

    # boring_viewer = strict_superset_df.filter(pl.col('pmid').is_in(boring_pmids))
    # boring_viewer.write_csv('data/_compiled/compare/brenda-strict-superset-boring-apogee.tsv', separator='\t')

    # now, turn to off_by_unit_df
    # we want any pmid with at least 1 non-null kcat_feedback


    # off_by_kcat_unit_pmids = off_by_unit_df.filter(pl.col('kcat_feedback').is_not_null())['pmid'].unique().to_list()
    # off_by_kcat_unit_df = off_by_unit_df.filter(pl.col('pmid').is_in(off_by_kcat_unit_pmids))

    # off_by_kcat_unit_df.write_csv('data/_compiled/compare/brenda-off-by-kcat-unit-apogee.tsv', separator='\t')

    from enzyextract.utils.pmid_management import lift_pmids
    # lift_pmids(off_by_kcat_unit_pmids, 'D:/topoff', 'C:/conjunct/tmp/kcat_unit_apogee')
    # lift_pmids(off_by_kcat_unit_pmids, 'D:/wos', 'C:/conjunct/tmp/kcat_unit_apogee')
    # lift_pmids(off_by_kcat_unit_pmids, 'D:/brenda', 'C:/conjunct/tmp/kcat_unit_apogee')
    # lift_pmids(off_by_kcat_unit_pmids, 'D:/scratch', 'C:/conjunct/tmp/kcat_unit_apogee')

    # off_by_km_unit_pmids = off_by_unit_df.filter(pl.col('km_feedback').is_not_null())['pmid'].unique().to_list()
    # off_by_km_unit_df = off_by_unit_df.filter(pl.col('pmid').is_in(off_by_km_unit_pmids))

    # off_by_km_unit_df.write_csv('data/_compiled/compare/brenda-off-by-km-unit-apogee.tsv', separator='\t')
    off_by_km_unit_df = pl.read_csv('data/_compiled/compare/brenda-off-by-km-unit-apogee.tsv', separator='\t')
    off_by_km_unit_pmids = off_by_km_unit_df['pmid'].unique().to_list()
    lift_pmids(off_by_km_unit_pmids, 'D:/topoff', 'C:/conjunct/tmp/km_unit_apogee')
    lift_pmids(off_by_km_unit_pmids, 'D:/wos', 'C:/conjunct/tmp/km_unit_apogee')
    lift_pmids(off_by_km_unit_pmids, 'D:/brenda', 'C:/conjunct/tmp/km_unit_apogee')
    lift_pmids(off_by_km_unit_pmids, 'D:/scratch', 'C:/conjunct/tmp/km_unit_apogee')

