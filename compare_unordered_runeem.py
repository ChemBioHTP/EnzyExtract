import os
import polars as pl

from kcatextract.hungarian.set_matching import match_by_unique_kcat_km
from kcatextract.hungarian.csv_fix import pl_prep_brenda_for_hungarian
from kcatextract.hungarian.csv_fix import widen_df, pl_widen_df, pl_remove_ranges
from kcatextract.utils.pmid_management import pmids_from_cache



def split_supersets(df):
    """
    Get these: 
    df1 only (set1 nonnull, set2 null)
    df2 only (set1 null, set2 nonnull)
    then, calculate the ratio max(set1, set2) / min(set1, set2)
    """

    df1_only = df.filter(pl.col('set1').is_not_null() & pl.col('set2').is_null())
    df2_only = df.filter(pl.col('set1').is_null() & pl.col('set2').is_not_null())
    both = df.filter(pl.col('set1').is_not_null() & pl.col('set2').is_not_null())
    
    # set 'ratio' column into both

    # both = both.with_columns([
    #     (pl.max_horizontal(['set1', 'set2']) / 
    #     pl.min_horizontal(['set1', 'set2'])).alias('ratio')
    # ])
    return df1_only, df2_only, both



def compute_metrics(df_paired):
    """
    Compute the metrics for a df_paired, which is produced by match_by_unique_kcat_km. 

    TP: true positives (common to both sets, and the value is close)
    FP: false positives (unique to set1)
    FN: false negatives (unique to set2)
    wrong: common to both sets, but the value is not close
    off: common to both sets, and the value is off by a known factor
    """
    df1_only, df2_only, both = split_supersets(df_paired)

    for dtype in ['kcat', 'km']:
        df1 = df1_only.filter(pl.col('col_name') == dtype)
        df2 = df2_only.filter(pl.col('col_name') == dtype)
        b = both.filter(pl.col('col_name') == dtype).with_row_index('index')
        FP = df1.height
        FN = df2.height

        TP = b.filter(pl.col('ratio') < 1.05).height
        # off1000: ratio is very close to 1000 (within 0.5)

        # TP: only if the ratio < 1.05
        print(f"Type: {dtype}")
        print("  FP:", FP)
        print("  FN:", FN)
        print("  TP:", TP)


        offs = []
        if dtype == 'kcat':
            off60 = b.filter((59 < pl.col('ratio')) & (pl.col('ratio') < 61))
            off3600 = b.filter((3599 < pl.col('ratio')) & (pl.col('ratio') < 3601))
            print("  off by 60:", off60.height)
            print("  off by 3600:", off3600.height)
            offs.append(off60)
            offs.append(off3600)
            for i in range(1, 7):
                off = b.filter((10**i-.5 < pl.col('ratio')) & (pl.col('ratio') < 10**i+.5))
                print(f"  off by 10^{i}:", off.height)
                offs.append(off)
        elif dtype == 'km':
            # check off by 10, 100, 1000, 10000
            for i in range(1, 7):
                off = b.filter((10**i-.5 < pl.col('ratio')) & (pl.col('ratio') < 10**i+.5))
                print(f"  off by 10^{i}:", off.height)
                offs.append(off)
        # get the remainder, and call it "wrong"
        all_offs = pl.concat(offs)

        all_wrong = b.filter(pl.col('ratio') > 1.05)

        # remove the offs from wrong
        wrong = all_wrong.filter(~pl.col('index').is_in(all_offs['index']))
        
        print("  wrong:", wrong.height)

        print("  Precision (assume no FP):", TP / (TP + all_wrong.height))
        print("  Precision:", TP / (TP + FP + all_wrong.height))
        print("  Recall:", TP / (TP + FN))
        print("  Accuracy:", TP / (TP + FP + FN + all_wrong.height))
        

        print()





# write_dest = None #'data/_compiled/apogee-brenda-unordered.tsv'

# df1 = pl.read_csv('data/_compiled/apogee-brenda-and-nonbrenda.tsv', separator='\t', 
#                     schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8})

# df2 = pl.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv', 
#                     schema_overrides={'pmid': pl.Utf8, 'km_value': pl.Utf8, 'turnover_number': pl.Utf8, 'kcat_km': pl.Utf8})

brenda = pl.read_csv('C:/conjunct/vandy/yang/corpora/brenda/brenda_km_kcat_key_v2.csv', 
                    schema_overrides={'pmid': pl.Utf8, 'km_value': pl.Utf8, 'turnover_number': pl.Utf8, 'kcat_km': pl.Utf8})
brenda = pl_prep_brenda_for_hungarian(brenda)
brenda = pl_remove_ranges(brenda)
brenda = pl.from_pandas(widen_df(brenda.to_pandas()))

so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}

# namespace = 'arctic-t2neboth'
namespace = 'beluga-t2neboth'
df1 = pl.read_csv(f'data/valid/_valid_{namespace}_1.csv', schema_overrides=so)
# remove pmids that were used to train
train_pmids = pmids_from_cache('finetunes/pmids-t2neboth.train')
df1 = df1.filter(~pl.col('pmid').is_in(train_pmids))

# namespace = 'arctic-brenda'
# df1 = brenda

df2 = pl.read_csv(f'data/humaneval/runeem/runeem_20241125.csv', schema_overrides=so)

write_dest = f"data/humaneval/comparisons/unordered_{namespace}_runeem.tsv"

if write_dest and not os.path.exists(write_dest):


    # rename df2
    # df2 = pl_prep_brenda_for_hungarian(df2)
    # df2 = widen_df(df2)

    df = match_by_unique_kcat_km(df1, df2, 'pmid')
    df.write_csv(write_dest, separator='\t')

else:

    df = pl.read_csv(write_dest, separator='\t', 
                        schema_overrides={'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8})

unique_pmids = df['pmid'].unique().to_list()
print("Namespace:", namespace)
print("Unique PMIDs:", len(unique_pmids))

compute_metrics(df)