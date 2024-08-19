
from kcatextract.hungarian.csv_fix import prep_for_hungarian, widen_df


def form_human_perfect(ground_df): # , whitelist: set=None):
    if 'pmid' not in ground_df.columns:
        ground_df = ground_df.rename(columns={'doi': 'pmid'})
    if 'pmid' in ground_df.columns:
        ground_df = ground_df.astype({'pmid': 'str'})
    if "enzyme_2" in ground_df.columns:
        # convert from checkpoint format
        # and assume that kcat_2 is correct
        ground_df = ground_df[["pmid","enzyme_2","pH_2","temperature_2","substrate_2","comments_2","km_2","kcat_2","kcat_km_2","mutant_2"]]
        # rename x_2 to x
        ground_df.columns = [x[:-2] if x.endswith('_2') else x for x in ground_df.columns]
        # the below calls fix_km and fix_kcat
        ground_df = prep_for_hungarian(ground_df)
        ground_df = widen_df(ground_df, brenda=True)
    elif "seq_first_10" in ground_df.columns:
        # this is the OLD annotation format
        # rename kcat/km to kcat_km
        ground_df = ground_df.rename(columns={'kcat/km': 'kcat_km'})
        ground_df = prep_for_hungarian(ground_df, printme=False)
        ground_df = widen_df(ground_df, brenda=True)
        # pmids = set(ground_df['pmid'])
        # if whitelist is not None:
        #     pmids &= whitelist
    return ground_df
    if pmids:
        matched_df = match_dfs_by_pmid(valid_df, ground_df, sorted(necessary_pmids), coefficients=coeffs)
        matched_df = convenience_rearrange_cols(matched_df)
        agreement, total = get_agreement_score(matched_df)
        print(f"Ground-truth Agreement score: ={agreement}/{total} which is ({agreement/total:.2f})")


# rekcat checkpoint 5 criteria:
# green pmid = take from km_2
# yellow pmid = values only from km_2
# -- to retain enzyme/substrate info, try hungarian matching and then 


# VERY INFORMATIVE:
# 10029307, 10819967, 10958794 is very informative
# 10671479
# 11209758 (microM omit)
# 11382747 (skipped row property)

# 11415449 mildly informative
# 11284731, 11790123, 11948179 (mix plaintext + table)
# 12604203 (ignore Vmax)

# 11284731 difficulty in substrate matching
# 11279139, 11382747: no molar mass means that Km is given in mg/mL

# from val set that actually I might want to move to train:
# 10748206 because prompt gpt not to repeat info found in 2 tables
# 10960102 give to fulltext
# 11917124 because of %


#  These are the true val set: (10)
# 10206992, 10347221, 10373434, 10947957, 10960485, 11016923, 11468288, 11675384, 12054464, 12604203
    