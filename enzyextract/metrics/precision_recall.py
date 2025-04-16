import math
from typing import Callable, Optional
import polars as pl
import polars.selectors as cs

from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit
from enzyextract.hungarian.pl_hungarian_match import join_optimally
from enzyextract.metrics.mantissa_distances import within_tolerance


def extract_value_and_unit_df(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Extract the unit column from the dataframe.
    
    If col_name == 'kcat', then the returned dataframe will have columns 'kcat.value', 'kcat.unit', and 'kcat.true_value'.
    """
    def _wrap_parse_value_and_unit(x):
        value, unit, _ = parse_value_and_unit(x)
        true_value = convert_to_true_value(value, unit)
        return {
            'value': float(value) if isinstance(value, int) else value,
            'unit': unit,
            'true_value': true_value,
            # 'log_true_value': math.log10(true_value) if true_value is not None and true_value > 0 else None,
        }
    df = df.with_columns(
        pl.col(col_name)
        .map_elements(
            _wrap_parse_value_and_unit,
            return_dtype=pl.Struct(
                {
                    'value': pl.Float64,
                    'unit': pl.Utf8,
                    'true_value': pl.Float64,
                    # 'log_true_value': pl.Float64,
                }
            )
        ).name.prefix_fields(col_name + '.').alias(col_name + '.')
    )
    df = df.unnest(col_name + '.')
    return df

def _join_closest_asof(gpt_df: pl.DataFrame, truth_df: pl.DataFrame, context_cols, val_col, tolerance=None) -> pl.DataFrame:
    """
    Example code of how to join the closest value in truth_df to each row in gpt_df.
    This is done using join_asof
    tolerance: only permit a join if it is within the tolerance
    """
    gpt_df = gpt_df.sort(*context_cols, val_col)
    truth_df = truth_df.sort(*context_cols, val_col)
    gpt_df = gpt_df.select(*context_cols, val_col)
    truth_df = truth_df.select(*context_cols, val_col)
    
    truth_pmids = truth_df.select('pmid').unique()
    gpt_df = gpt_df.filter(pl.col('pmid').is_in(truth_pmids['pmid']))
    joined_df = gpt_df.join_asof(
        truth_df,
        by=context_cols,
        on=val_col,
        tolerance=tolerance, 
        strategy='nearest',
        coalesce=False
    )
    return joined_df
    

def asof_precision_recall(
    gpt_df: pl.DataFrame,
    truth_df: pl.DataFrame,
    *, 
    on: str = None,
    left_on = None, 
    right_on = None,
    by=['pmid'],
    tolerance=1E-6,
    keep_all_columns=False,
):
    """
    Maximize values that are exactly within tolerance.
    """
    if on is None:
        assert left_on is not None and right_on is not None, "on must be specified if left_on and right_on are not specified"
    else:
        left_on = on
        right_on = on

    if keep_all_columns:
        gpt_subset = gpt_df
        truth_subset = truth_df
    else:
        gpt_subset = gpt_df.select(*by, left_on)
        truth_subset = truth_df.select(*by, right_on)

    if gpt_subset.schema[left_on] == pl.Utf8:
        # need to convert to float
        gpt_float = extract_value_and_unit_df(gpt_subset, left_on)
        left_on = f'{left_on}.true_value'
    else:
        assert gpt_subset.schema[left_on].is_numeric(), f"Unsupported type {gpt_subset.schema[left_on]} for {left_on}"
        gpt_float = gpt_subset
    
    if truth_subset.schema[right_on] == pl.Utf8:
        # need to convert to float
        truth_float = extract_value_and_unit_df(truth_subset, right_on)
        right_on = f'{right_on}.true_value'
    else:
        assert truth_subset.schema[right_on].is_numeric(), f"Unsupported type {truth_subset.schema[right_on]} for {right_on}"
        truth_float = truth_subset
    

    # a = 0 if a is None else a

    # remove None, as it is a waste of computation
    gpt_float = gpt_float.filter(pl.col(left_on).is_not_null())
    truth_float = truth_float.filter(pl.col(right_on).is_not_null())

    joined_df = gpt_float.join_asof(
        truth_float, 
        left_on=left_on, 
        right_on=right_on, 
        by=by, 
        strategy='nearest',
        coalesce=False,
        suffix='_2'
    )

    if left_on != right_on:
        # the value from the truth dataframe (the "right" value)
        right_on = f'{right_on}_2' 
    
    # calculate closeness
    joined_df = joined_df.with_columns(
        pl.when(
            (pl.col(left_on) > 0) & (pl.col(right_on) > 0)
        ).then(
            (pl.col(left_on) - pl.col(right_on)) / pl.min_horizontal(left_on, right_on).abs() < tolerance
        ).otherwise(
            pl.col(left_on) == pl.col(right_on)
        ).alias('is_close')
    )

    TP = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null() 
        & pl.col('is_close')
    )
    
    left_only = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_null()
        # & ~pl.col(col_is_close)
    )
    
     
    right_only = joined_df.filter(
        pl.col(left_on).is_null() &
        pl.col(right_on).is_not_null()
        # & ~pl.col(col_is_close)
    )
    
    wrong = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null()
        & ~pl.col('is_close')
    )
    
    TPnum = TP.height
    FPnum = left_only.height + wrong.height
    FNnum = right_only.height + wrong.height
    precision = TPnum / (TPnum + FPnum) if (TPnum + FPnum) > 0 else 0
    recall = TPnum / (TPnum + FNnum) if (TPnum + FNnum) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    dfs = {
        'joined': joined_df,
        'TP': TP,
        'FP': left_only,
        'FN': right_only,
        'wrong': wrong,
    }
    metrics = {
        'TP': TPnum,
        'FP': FPnum,
        'FN': FNnum,
        'precision': precision,
        'recall': recall,
        'f1': f1,
    }
    return dfs, metrics



def exact_precision_recall(
    gpt_df: pl.DataFrame,
    truth_df: pl.DataFrame,
    *, 
    on: str = None,
    left_on = None, 
    right_on = None,
    by=['pmid'],
    tolerance=1E-6,
    keep_all_columns=False,
):
    """
    Maximize values that are exactly within tolerance.
    """
    if on is None:
        assert left_on is not None and right_on is not None, "on must be specified if left_on and right_on are not specified"
    else:
        left_on = on
        right_on = on

    if keep_all_columns:
        gpt_subset = gpt_df
        truth_subset = truth_df
    else:
        gpt_subset = gpt_df.select(*by, left_on)
        truth_subset = truth_df.select(*by, right_on)

    if gpt_subset.schema[left_on] == pl.Utf8:
        # need to convert to float
        gpt_float = extract_value_and_unit_df(gpt_subset, left_on)
        left_on = f'{left_on}.true_value'
    else:
        assert gpt_subset.schema[left_on].is_numeric(), f"Unsupported type {gpt_subset.schema[left_on]} for {left_on}"
        gpt_float = gpt_subset
    
    if truth_subset.schema[right_on] == pl.Utf8:
        # need to convert to float
        truth_float = extract_value_and_unit_df(truth_subset, right_on)
        right_on = f'{right_on}.true_value'
    else:
        assert truth_subset.schema[right_on].is_numeric(), f"Unsupported type {truth_subset.schema[right_on]} for {right_on}"
        truth_float = truth_subset
    
    def exact_objective(row1, row2):
        left = row1[left_on]
        right = row2[right_on]

        return within_tolerance(left, right, tolerance=tolerance)

    # a = 0 if a is None else a

    # remove None, as it is a waste of computation
    gpt_float = gpt_float.filter(pl.col(left_on).is_not_null())
    truth_float = truth_float.filter(pl.col(right_on).is_not_null())

    joined_df = join_optimally(
        gpt_float,
        truth_float,
        objective_fn=exact_objective,
        partition_by=by,
        how='outer',
        progress_bar=True,
        maximize=True,
        objective_column='z'
    )

    if joined_df.height == 0:
        print("Warning: no commonality found.")
        dfs = {
            'joined': joined_df,
            'TP': joined_df,
            'FP': joined_df,
            'FN': joined_df,
            'wrong': joined_df,
        }
        metrics = {
            'num_pmids': 0,
            'TP': 0,
            'FP': 0,
            'FN': 0,
            'precision': 0.0,
            'recall': 0.0,
            'f1': 0.0,
        }
        return dfs, metrics

    left_on = f'{left_on}_1'
    right_on = f'{right_on}_2' # the value from the truth dataframe (the "right" value)

    TP = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null() 
        & (pl.col('z') == 1)
    )
    
    left_only = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_null()
        # & ~pl.col(col_is_close)
    )
    
     
    right_only = joined_df.filter(
        pl.col(left_on).is_null() &
        pl.col(right_on).is_not_null()
        # & ~pl.col(col_is_close)
    )
    
    wrong = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null()
        # ~pl.col(col_is_close)
        & (pl.col('z') == 0)
    )
    
    TPnum = TP.height
    FPnum = left_only.height + wrong.height
    FNnum = right_only.height + wrong.height
    # Note that false negatives (FN) do not make sense in our context

    precision = TPnum / (TPnum + FPnum) if (TPnum + FPnum) > 0 else 0
    recall = TPnum / (TPnum + FNnum) if (TPnum + FNnum) > 0 else 0
    accuracy = TPnum / (TPnum + FPnum + FNnum) if (TPnum + FPnum + FNnum) > 0 else 0
    
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    num_pmids = set(gpt_df['pmid']).intersection(set(truth_df['pmid']))

    dfs = {
        'joined': joined_df,
        'TP': TP,
        'FP': left_only,
        'FN': right_only,
        'wrong': wrong,
    }
    metrics = {
        'num_pmids': len(num_pmids),
        'TP': TPnum,
        'FP': FPnum,
        'FN': FNnum,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'accuracy': accuracy,
    }
    return dfs, metrics

def offby_matches(
    gpt_float: pl.DataFrame,
    truth_float: pl.DataFrame,
    offby_seq: Optional[list],
    *, 
    joined_df: pl.DataFrame = None,
    on: str = None,
    left_on = None, 
    right_on = None,
    by=['pmid'],
    left_spectators=None,
    right_spectators=None,
    tolerance=1E-6,
    keep_all_columns=False,
    symmetric=True,
):
    """
    spectators: list of spectator columns to include. Default (None): include all.
    Can be polars selectors.
    """
    if isinstance(by, str) and not isinstance(by, list):
        raise ValueError("by must be a list")
        # by = [by]
        
    """
    similar to exact_precision_recall, but uses a sequence of off-by values to match.
    returns solely the "dfs" portion, which is a dictionary that maps
    {
        '1000': df
        '1000000': df
        '100': df
        'remainder': df
    }
    """
    if gpt_float is None or truth_float is None:
        assert joined_df is not None, "gpt_df and truth_df must be provided if joined_df is not provided"
        # only care about offby matches
        joined_df = joined_df.filter(
            pl.col('z').is_null() |
            (pl.col('z') != 1)
        )
        gpt_float = joined_df.select(*by, cs.ends_with('_1')).rename({
            x: x[:-2] for x in joined_df.columns if x.endswith('_1')
        })
        truth_float = joined_df.select(*by, cs.ends_with('_2')).rename({
            x: x[:-2] for x in joined_df.columns if x.endswith('_2')
        })


    if offby_seq is None:
        offby_seq = [10**3, 10**6, 10**9, 10**4, 10**5, 10**7, 10**8]
    
    if on is None:
        assert left_on is not None and right_on is not None, "on must be specified if left_on and right_on are not specified"
    else:
        left_on = on
        right_on = on
    
    def offby_objective(row1, row2, offby, symmetric=True):
        left = row1[left_on]
        right = row2[right_on]

        if left is None and right is None:
            return True
        if left is None or right is None:
            return False
        result = within_tolerance(left, right * offby)
        if symmetric:
            result = result or within_tolerance(left * offby, right)
        return result

    # a = 0 if a is None else a

    # remove None, as it is a waste of computation
    gpt_float = gpt_float.filter(pl.col(left_on).is_not_null())
    truth_float = truth_float.filter(pl.col(right_on).is_not_null())

    if left_spectators is None:
        left_spectators = [cs.exclude(*by, left_on)]
    if right_spectators is None:
        right_spectators = [cs.exclude(*by, right_on)]
    left_df = gpt_float.select(*by, left_on, *left_spectators)
    right_df = truth_float.select(*by, right_on, *right_spectators)
    left_df = left_df.filter(pl.col(left_on).is_not_null())
    right_df = right_df.filter(pl.col(right_on).is_not_null())
    
    prev_height = left_df.height + right_df.height
    collector = {}
    for offby in offby_seq:
        if joined_df.height == 0:
            collector[str(offby)] = joined_df.clear()
            continue
            # break
        joined_df = join_optimally(
            left_df,
            right_df,
            objective_fn=lambda a, b: offby_objective(a, b, offby=offby_seq[0], symmetric=symmetric),
            partition_by=by,
            how='outer',
            progress_bar=False,
            maximize=True,
            objective_column='z'
        )
        offby_df = joined_df.filter(pl.col('z') == 1)
        collector[str(offby)] = offby_df

        joined_df = joined_df.filter(
            pl.col('z').is_null() |
            (pl.col('z') != 1)
        )


        # split again
        left_df = joined_df.select(*by, cs.ends_with('_1')).rename({
            x: x[:-2] for x in joined_df.columns if x.endswith('_1')
        })
        right_df = joined_df.select(*by, cs.ends_with('_2')).rename({
            x: x[:-2] for x in joined_df.columns if x.endswith('_2')
        })
        left_df = left_df.filter(pl.col(left_on).is_not_null())
        right_df = right_df.filter(pl.col(right_on).is_not_null())

        curr_height = 2 * offby_df.height + left_df.height + right_df.height 
        # number of non-null entries should stay the same
        if curr_height != prev_height:
            print("Assertion failed: we lost material")
            pass
        prev_height = left_df.height + right_df.height
    collector['left_only'] = left_df
    collector['right_only'] = right_df
    return collector
        



def _exact_precision_recall_by_norm(
        gpt_df: pl.DataFrame, 
        truth_df: pl.DataFrame, 
        col,
        # by_unit=False,
        tolerance=0.05) -> pl.DataFrame:
    """
    Calculate the precision and recall of the dataframe using exact matching.
    Tolerance is the maximum allowed difference between the predicted and ground truth values.
    
    Note: to make the tolerance work across multiple orders of magnitude, I recommend using log values.
    
    """
    # context_cols = ['pmid']
    # if by_unit:
    #     val_col = 'kcat.value'
    #     context_cols.append('kcat.unit')
    # else:
    # log_col = f'{val_col}.log_true_value'
    if col not in gpt_df.columns:
        raise ValueError(f"Column {col} not found in gpt_df")
    if col not in truth_df.columns:
        raise ValueError(f"Column {col} not found in truth_df")
    
    joined_df = join_optimally(
        gpt_df,
        truth_df,
        objective_fn=lambda a, b: norm_sq(a[col], b[col]),
        partition_by='pmid',
        how='inner',
        progress_bar=True,
        maximize=False, # we minimize the norm_sq distance
    )

    
    
    # Calculate precision and recall
    left_on = f'{col}_1'
    right_on = f'{col}_2' # the value from the truth dataframe (the "right" value)

    col_is_close = f'metrics.{col.split(".")[0]}.is_close'
    joined_df = joined_df.with_columns(
        ((pl.col(left_on) - pl.col(right_on)).abs() / pl.min_horizontal(left_on, right_on).abs() < tolerance).alias(col_is_close)
    )

    
    TP = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null() &
        pl.col(col_is_close)
    )
    
    FP = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_null()
        # & ~pl.col(col_is_close)
    )
    
     
    FN = joined_df.filter(
        pl.col(left_on).is_null() &
        pl.col(right_on).is_not_null()
        # & ~pl.col(col_is_close)
    )
    
    wrong = joined_df.filter(
        pl.col(left_on).is_not_null() &
        pl.col(right_on).is_not_null() &
        ~pl.col(col_is_close)
    )
    
    return joined_df, TP, FP, FN

