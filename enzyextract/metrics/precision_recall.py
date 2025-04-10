import math
import polars as pl

from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit
from enzyextract.hungarian.pl_hungarian_match import join_optimally


def extract_value_and_unit_df(df: pl.DataFrame, col_name: str) -> pl.DataFrame:
    """
    Extract the unit column from the dataframe.
    
    If col_name == 'kcat', then the returned dataframe will have columns 'kcat.value', 'kcat.unit', and 'kcat.true_value'.
    """
    def _wrap_parse_value_and_unit(x):
        value, unit, _ = parse_value_and_unit(x)
        true_value = convert_to_true_value(value, unit)
        return {
            'value': value,
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
    

def _asof_exact_precision_recall(
        gpt_df: pl.DataFrame, 
        truth_df: pl.DataFrame, 
        # by_unit=False,
        tolerance=0.05) -> pl.DataFrame:
    """
    Calculate the precision and recall of the dataframe using exact matching.
    Tolerance is the maximum allowed difference between the predicted and ground truth values.
    
    Unfortunately, polars asof "nearest" will not work because it can map multiple truth values to the same gpt value.
    
    Note: to make the tolerance work across multiple orders of magnitude, I recommend using log values.
    
    """
    context_cols = ['pmid']
    # if by_unit:
    #     val_col = 'kcat.value'
    #     context_cols.append('kcat.unit')
    # else:
    val_col = 'kcat.log_true_value'
    right_val_col = val_col + '_right' # the value from the truth dataframe (the "right" value)
    
    if val_col not in gpt_df.columns:
        gpt_df = extract_value_and_unit_df(gpt_df, 'kcat')
    if right_val_col not in truth_df.columns:
        truth_df = extract_value_and_unit_df(truth_df, 'kcat')
    
    joined_df = _join_closest_asof(
        gpt_df, 
        truth_df, 
        context_cols=context_cols, 
        val_col=val_col,
        tolerance=tolerance)
    
    
    # Calculate precision and recall
    
    
    TP = joined_df.filter(
        pl.col(val_col).is_not_null() &
        pl.col(right_val_col).is_not_null()
        # pl.col(f'metrics.{val_col}.is_close')
    )
    
    FP = joined_df.filter(
        pl.col(val_col).is_not_null() &
        pl.col(right_val_col).is_null()
    )
    
     
    FN = joined_df.filter(
        pl.col(val_col).is_null() &
        pl.col(right_val_col).is_not_null()
    )
    
    # wrong = joined_df.filter(
    #     pl.col(val_col).is_not_null() &
    #     pl.col(right_val_col).is_not_null() &
    #     pl.col(f'metrics.{val_col}.is_close').is_false()
    # )
    
    return joined_df, TP, FP, FN


def norm_sq(x, y):
    x = 0 if x is None else x
    y = 0 if y is None else y
    return (x - y)**2

def exact_precision_recall(
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
    left_col = f'{col}_1'
    right_col = f'{col}_2' # the value from the truth dataframe (the "right" value)

    col_is_close = f'metrics.{col.split(".")[0]}.is_close'
    joined_df = joined_df.with_columns(
        ((pl.col(left_col) - pl.col(right_col)).abs() / pl.min_horizontal(left_col, right_col).abs() < tolerance).alias(col_is_close)
    )

    
    TP = joined_df.filter(
        pl.col(left_col).is_not_null() &
        pl.col(right_col).is_not_null() &
        pl.col(col_is_close)
    )
    
    FP = joined_df.filter(
        pl.col(left_col).is_not_null() &
        pl.col(right_col).is_null()
        # & ~pl.col(col_is_close)
    )
    
     
    FN = joined_df.filter(
        pl.col(left_col).is_null() &
        pl.col(right_col).is_not_null()
        # & ~pl.col(col_is_close)
    )
    
    wrong = joined_df.filter(
        pl.col(left_col).is_not_null() &
        pl.col(right_col).is_not_null() &
        ~pl.col(col_is_close)
    )
    
    return joined_df, TP, FP, FN

