from typing import Tuple, Union
import polars as pl

from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress
def filter_out(regex: str, remf: pl.DataFrame, *, on='fragments', extract=False) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Filter a DataFrame by a regex pattern.
    """
    mask = remf.select(
        pl.col(on).str.contains(regex)
    ).fill_null(False).to_series()
    subset = remf.filter(mask)
    remf = remf.filter(~mask)

    if extract:
        subset = subset.with_columns(
            pl.col(on).str.extract(regex, 0).alias('extract')
        ) # .drop('shrinkable', strict=False)
    return subset, remf


def subfilter_out(regex: str, remf: Union[pl.DataFrame, pl.LazyFrame], drop_shrinkable=True) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Filter a DataFrame by a regex pattern, and extract substrings
    """
    remf = remf.lazy()

    assert not regex.endswith('$')

    # subset = remf.filter(remf['shrinkable'].str.contains(regex))

    # remove additional punctuation
    regex_out = rf"\(?{regex}\)?(, | ?\/ ?)?"

    subset = remf.with_columns(
        pl.col('shrinkable').str.extract(regex, 0).alias('extract'),
        # pl.col('shrinkable').str.replace(regex_out, '').alias('rest')
    ).drop_nulls('extract').drop('fragment_lower', strict=False)
    if drop_shrinkable:
        subset = subset.drop('shrinkable')

    remf = remf.with_columns(
        pl.col('shrinkable')
        .str.replace(regex_out, '')
        # .str.strip_suffix(', ')
        .alias('shrinkable')
    ).filter(
        pl.col('shrinkable').str.len_chars() > 0 # remove empty strings
    )
    return subset.collect(), remf.collect()

def join_out(rhs: pl.DataFrame, remf: pl.DataFrame, right_on: str, **kwargs) -> Tuple[pl.DataFrame, pl.DataFrame]:
    remf_lazy = remf.lazy()
    subset = remf_lazy.join(
        rhs.lazy(),
        left_on=['context_id', 'fragments'],
        right_on=['context_id', right_on],
        how='inner',
        **kwargs
    ).drop('shrinkable', strict=False).drop('fragment_lower', strict=False)
    # remf = remf.filter(~pl.col('fragment_id').is_in(set(subset['fragment_id'])))
    remf = remf_lazy.join(
        subset,
        on='fragment_id',
        how='anti',
    )
    return subset.collect(), remf.collect()

def join_out_lower(rhs: pl.DataFrame, remf: Union[pl.LazyFrame, pl.DataFrame], right_on: str, **kwargs) -> Tuple[pl.DataFrame, pl.DataFrame]:
    remf = remf.lazy()
    subset = remf.join(
        rhs.lazy(),
        left_on=['context_id', 'fragment_lower'],
        right_on=['context_id', right_on],
        how='inner',
        **kwargs
    ).drop('fragment_lower')
    # remf = remf.filter(~pl.col('fragment_id').is_in(set(subset['fragment_id'])))
    remf = remf.join(
        subset,
        on='fragment_id',
        how='anti',
    )
    return subset.collect(), remf.collect()

def substring_by_column(df: pl.DataFrame, haystack_col: str, needle_col: str) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col(needle_col).is_not_null()).then(
            pl.col(haystack_col)
            # .str.replace(pl.col('extract'), '', literal=True)
            # https://github.com/pola-rs/polars/issues/14367
            .str.replace(pl.col(needle_col).first(), '', literal=True).over(needle_col)
            .str.replace(r', (, |$)', '') # remove extra commas
            .str.replace(r' \(\)', '') # remove extra parentheses
            .str.strip_chars()
            .alias(haystack_col)
        ).otherwise(pl.col(haystack_col))
    ).filter(
        pl.col(haystack_col).str.len_chars() > 0 # remove empty strings
    )
    return df

def subjoin_out(rhs: pl.DataFrame, remf_eager: pl.DataFrame, right_on: str, *, remf_on='shrinkable', drop_shrinkable=True, **kwargs) -> pl.DataFrame:
    """
    Extracts substrings (described by rhs) from remf. Uses the 'shrinkable' column.
    So rhs should contain shorter strings.
    
    If multiple matches are found, the first one is used. 
    (A recommendation: sort rhs by decreasing length, so the longest substring is used.)

    Outputs in 'extract' column.
    """
    remf = remf_eager.lazy()
    product = remf.join(
        rhs.lazy(),
        left_on=['context_id'],
        right_on=['context_id'],
        how='inner',
        **kwargs
    )
    subset = product.filter(
        pl.col(remf_on).str.contains(pl.col(right_on), literal=True)
    ).rename({
        right_on: 'extract'
    }) # .drop(right_on)
    successful_matches = subset.select(
        'fragment_id', 
        'extract'
    ).unique('fragment_id', keep='first')
    successful_remf = remf.join(successful_matches, on='fragment_id', validate='m:1', how='inner', coalesce=True)
    rest_remf = remf.join(successful_matches, on='fragment_id', how='anti')

    # successful_remf = successful_remf.with_columns(
    #     pl.when(pl.col('extract').is_not_null()).then(
    #         pl.col('shrinkable')
    #         # .str.replace(pl.col('extract'), '', literal=True)
    #         # https://github.com/pola-rs/polars/issues/14367
    #         .str.replace(pl.col('extract').first(), '', literal=True).over('extract')
    #         .str.replace(r', (, |$)', '') # remove extra commas
    #         .str.strip_chars()
    #         .alias('shrinkable')
    #     ).otherwise(pl.col('shrinkable'))
    # ).filter(
    #     pl.col('shrinkable').str.len_chars() > 0 # remove empty strings
    # )
    successful_remf = substring_by_column(successful_remf, remf_on, 'extract').drop('extract')
    remf = rest_remf.merge_sorted(successful_remf, key='fragment_id')
    subset = subset.drop('fragment_lower', strict=False)
    if drop_shrinkable:
        subset = subset.drop(remf_on)
    return subset.collect(), remf.collect()



def fuzzyjoin_out(
        rhs: pl.DataFrame, 
        remf: pl.DataFrame, 
        right_on: str, 
        *, 
        remf_on='shrinkable', 
        threshold=100.0, 
        case_insensitive=True, 
        remf_min_length=0,
        # produce_shorter=True,
        **kwargs) -> pl.DataFrame:
    """
    Extract fuzzy matches between remf and rhs.
    Select those where the partial ratio is above the threshold.
    
    If multiple matches are found, the **multiple** are returned. 
    (A recommendation: sort rhs by decreasing length, so the longest substring comes first.)

    Output columns {remf_on} and {right_on} are unchanged from the input.
    """


    product = (
        remf
        if remf_min_length <= 0
        else remf.filter(pl.col(remf_on).str.len_chars() >= remf_min_length)
    ).join(
        rhs,
        left_on=['context_id'],
        right_on=['context_id'],
        how='inner',
        **kwargs
    )
    sim_scores = compute_fuzz_with_progress(
        product, 
        [
            (remf_on, right_on, not case_insensitive, 'partial_ratio'),
        ]
    )

    successful_matches = sim_scores.filter(
        pl.col('partial_ratio') >= threshold
    )

    rest_remf = remf.join(successful_matches, on='fragment_id', how='anti')

    # if produce_shorter:
    #     successful_matches = successful_matches.with_columns(
    #         pl.when(pl.col(remf_on).str.len_chars() > pl.col(right_on).str.len_chars())
    #         .then(pl.col(remf_on))
    #         .otherwise(pl.col(right_on)).alias('shorter')
    #     )

    return successful_matches, rest_remf

def pick_longest_string(plcol: pl.Expr) -> pl.Expr:
    """
    From a column of list of strings, pick the longest string.
    If list is empty, return null.
    """
    return plcol.list.get(
        plcol.list.eval(
            pl.element().str.len_chars().arg_max()
        ).list.get(0, null_on_oob=True) # get the index of the longest string
    ) # get it from the list

def filter_list_out(many: list[str], remf: pl.DataFrame, ascii_case_insensitive=False) -> pl.DataFrame:
    """
    Filter a DataFrame by a list of strings.
    TODO: what doesn't work is respecting word boundaries.
    """

    # temporarily add space to shrinkable, as a form of word boundary respecting
    remf = remf.with_columns(
        (' ' + pl.col('shrinkable') + ' ').alias('shrinkable')
    )
    subset = remf.with_columns(
        pick_longest_string(
            pl.col('shrinkable').str.extract_many(many, overlapping=True, ascii_case_insensitive=ascii_case_insensitive)
        ).alias('extract')
    ).drop('literal', strict=False)
    rest_remf = subset.filter(
        pl.col('extract').is_null()
    )
    subset = subset.filter(
        pl.col('extract').is_not_null()
    )

    successful_remf = substring_by_column(subset, 'shrinkable', 'extract')
    remf = rest_remf.merge_sorted(successful_remf, key='fragment_id')
    remf = remf.with_columns(
        pl.col('shrinkable').str.strip_chars() # remove space added to shrinkable
    )
    remf = remf.drop('extract')
    return subset, remf
