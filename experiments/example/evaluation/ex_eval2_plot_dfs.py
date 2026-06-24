from colorama import Fore, Style
import polars as pl
from enzyextract.metrics.precision_recall import paired_precision_recall, to_df_dual
from enzyextract.pipeline.evaluation.eval1_compare_dfs import load_rumble_df

def analyze_PR(matched_view: pl.DataFrame):

    goodall = matched_view.filter(
        pl.col('same_enzyme') &
        pl.col('same_substrate')
    )
    more_wrong = matched_view.filter(
        ~pl.col('same_enzyme') |
        ~pl.col('same_substrate')
    )
    dual_df = to_df_dual(goodall)
    paired_precision_recall(dual_df)

    print("Excluded kcat:", more_wrong.filter(
        # 1 from
        pl.col('kcat_1').is_null() | pl.col('kcat_2').is_null()
    ).height)
    print("Excluded km:", more_wrong.filter(
        # 2 from
        pl.col('km_1').is_null() | pl.col('km_2').is_null()
    ).height)

def _transpose(df):
    """
    Switch _1 <=> _2
    """
    # Get all columns that end with _1 or _2
    cols_1 = [col for col in df.columns if col.endswith('_1')]
    cols_2 = [col for col in df.columns if col.endswith('_2')]
    
    # Create a mapping of _1 columns to their _2 counterparts
    rename_map = {}
    for col1 in cols_1:
        base_name = col1[:-2]  # Remove _1
        col2 = f"{base_name}_2"
        if col2 in cols_2:
            rename_map[col1] = col2
            rename_map[col2] = col1
    
    # Rename the columns
    return df.rename(rename_map)

if __name__ == '__main__':
    # raise NotImplementedError("This script is only an example.")

    # working = 'thedata'
    # working = 'pruned'
    # working = 'unpruned'
    # working = 'rumble'
    # working = 'thedata'


    # against = 'rumble'
    # against = 'brenda'
    # against = 'sabiork'

    (working, against) = ('rumble', 'brenda')
    # (working, against) = ('pruned', 'rumble')

    scino_only = None
    # scino_only = True
    # scino_only = False
    # scino_only = 'false_revised'

    if scino_only is True:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':
        working += '_no_scientific_revised'
    
    # readme = f'data/matched/EnzymeSubstrate/{against}/{against}_{working}.parquet'
    readme = f'data/metrics/{against}/{against}_{working}.parquet'
    matched_view = pl.read_parquet(readme)

    if working == 'rumble' and against == 'brenda':
        # consider rumble as GT
        matched_view = _transpose(matched_view)
    # matched_view = matched_view.filter(
    #     pl.col('pmid') != '21980421'
    # )
    print("Reading", readme)
    # analyze_correlations(matched_view, f"1. {working} 2. {against}")
    
    print(f"{Fore.BLUE}PR, {working} against {against} {Style.RESET_ALL}")
    analyze_PR(matched_view)

    # NOTE: Of the 223 documents that comprise the ground truth evaluation set,
    # 130 were sought out because kcat values were erroneously not reported by BRENDA. 
    # To create a more representative evaluation, 
    # these documents are excluded from the kcat FN and accuracy metrics. Otherwise, FN=882.

    rumble =  load_rumble_df(exclude_train=True, exclude_rekcat=True)
    matched_view_filtered = matched_view.join(
        rumble.select('pmid'),
        on='pmid',
        how='semi'
    ) # filter out rekcat

    print(f"{Fore.BLUE}PR, {working} against {against} [NO REKCAT] {Style.RESET_ALL}")
    analyze_PR(matched_view_filtered)

    print("Adding rekcat to excluded values", 1630 + 882 - 23) # 2489
    # (excluded kcat filtered) + (rekcat FN) - (filtered FN)