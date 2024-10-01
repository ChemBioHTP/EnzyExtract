import pandas as pd


def count_enzyme_substrate_all_matched(checkpoint_df: pd.DataFrame, how='pmid'):
    # get all pmids where enzyme and substrate are matched
    result = []
    if how == 'pmid':
        pmids = checkpoint_df['pmid'].unique()
        for pmid in pmids:
            df = checkpoint_df[checkpoint_df['pmid'] == pmid]
            for row in df.itertuples():
                if not pd.isna(row.km) and (pd.isna(row.enzyme) or pd.isna(row.substrate)):
                    break
            else:
                result.append(pmid)
    elif how == 'rows':
        for i, row in checkpoint_df.iterrows():
            if pd.isna(row.enzyme) or pd.isna(row.substrate) or pd.isna(row.km):
                continue
            result.append(i)
    else:
        raise ValueError("how must be 'pmid' or 'rows'")
    return result
        
def broad_na(x):
    # note that for km or kcat, not na will also usually correspond to having a numeric value
    # because fix_km and fix_kcat will convert non-numeric and weird values to na
    if x is None:
        return True
    if isinstance(x, list):
        return x == []
    if isinstance(x, str):
        if x == 'nan':
            return True
        return x == ""
    if pd.isna(x):
        return True
    if not x:
        return True
    return False
    #     return pd.isna(x)
    # return pd.isna(x) or (not x)

def is_numlike(x):
#     # this is necessary, becauase although we guarantee that
#     # hung. matched dfs will have fixed km and kcat, 
#     # the ground truth may actually have non-numeric values 
#     # (ie. "NA" or "")
    return not broad_na(x) and any([c.isdigit() for c in str(x)])


# current_best = pd.read_csv('completions/enzy/rekcat-vs-brenda_5.csv')
# a pmid is considered "perfect" if all of these are true:
# 1. for every row of a pmid, km is non-null and km_2 is non-null and km_feedback is null
def get_perfects_only(checkpoint_df: pd.DataFrame, conditions: dict = None, allow_superset=True):

    
    if checkpoint_df.empty:
        return checkpoint_df
    
    pmids = checkpoint_df['pmid'].unique()
    perfect_pmids = []
    
    conditions = conditions if conditions is not None else {}
    for pmid in pmids:
        df = checkpoint_df[checkpoint_df['pmid'] == pmid]
        for row in df.itertuples():
            if broad_na(row.km):
                if not broad_na(row.km_2):
                    break
                else:
                    # both null? then we're good
                    pass
                # Invalid under these conditions:
                # 1. km is null and km_2 is not null (always)
            elif not allow_superset and broad_na(row.km_2):
                # When we disallow superset, this 
                # is also invalid:
                # 2. km is not null and km_2 is null
                break
            if not broad_na(row.km_feedback):
                if conditions.get('off by 1000') and row.km_feedback == "off by 1000":
                    continue # this is ok
                break
            if not broad_na(row.kcat_feedback):
                break
        else:
            perfect_pmids.append(pmid)
    
    # get subset of current_best that has perfect_pmids
    perfect_df = checkpoint_df[checkpoint_df['pmid'].isin(perfect_pmids)]
    
    return perfect_df


def get_agreement_score(checkpoint_df: pd.DataFrame, allow_brenda_missing=True):
    
    # this assumes a perfect ground-truth, so 
    def is_good_enough(row):
        # km and km_2 need to exactly match in na-ness
        # same for kcat and kcat/km
        # then, we need at least 1 non-null value
        # also, need that kcat_feedback and km_feedback are both empty
        
        if is_numlike(row.km) != is_numlike(row.km_2):
            return False
        if is_numlike(row.kcat) != is_numlike(row.kcat_2):
            if allow_brenda_missing and not is_numlike(row.kcat_2):
                pass
            else:
                return False
        if is_numlike(row.kcat_km) != is_numlike(row.kcat_km_2):
            if allow_brenda_missing and not is_numlike(row.kcat_km_2):
                pass
            else:
                return False
        if broad_na(row.km) and broad_na(row.kcat) and broad_na(row.kcat_km):
            return False
        return broad_na(row.km_feedback) and broad_na(row.kcat_feedback)
    
        
    agreement = 0
    total = 0
    for i, row in checkpoint_df.iterrows():
        
        # out of total:
        # only count where brenda has nonnull km, kcat, kcat_km
        if broad_na(row.km_2) and broad_na(row.kcat_2) and broad_na(row.kcat_km_2):
            continue
        
        total += 1
        if is_good_enough(row):
            agreement += 1
    
    return agreement, total
    
    # agreement = sum([is_good_enough(row) for i, row in checkpoint_df.iterrows()])
    
    # out of this total: all these conditions:
    # row has nonnull {km, kcat, kcat_km, km_2, kcat_2, kcat_km_2}
    # (so drop those na)
    # nonna = checkpoint_df.dropna(subset=['km', 'kcat', 'kcat_km', 'km_2', 'kcat_2', 'kcat_km_2'], how='all')
    # return agreement, len(nonna)
    
### Script 1: get perfect from rekcat 4o-mini tableless

def script1():
    # latest_ver = latest_version('completions/enzy_improve', 'rekcat-tableless', '.csv')
    df = pd.read_csv(f'completions/enzy_improve/rekcat-tableless_2.csv')
    
    perfect_df = get_perfects_only(df)
    print(len(perfect_df['pmid'].unique())) # 134
    
    # save
    perfect_df.to_csv('completions/enzy_improve/rekcat-tableless-perfect.csv', index=False)

# condition: 
def script2():
    df = pd.read_csv(f'completions/enzy_improve/rekcat-tableless_2.csv')
    
    perfect_df = get_perfects_only(df, conditions={'off by 1000': True})
    print(len(perfect_df['pmid'].unique())) # 149
    
    print("out of", len(df['pmid'].unique()))
    
    # write to backform/checkpoints/perfect_checkpoints.txt
    with open('backform/checkpoints/perfect_checkpoints.txt', 'w') as f:
        for pmid in perfect_df['pmid'].unique():
            f.write(str(pmid) + '\n')

    
    


if __name__ == "__main__":
    script2()
