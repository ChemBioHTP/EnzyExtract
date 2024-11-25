import polars as pl

from kcatextract.hungarian.hungarian_matching import parse_value_and_unit
from kcatextract.hungarian.hungarian_matching import convert_to_true_value
from kcatextract.metrics.get_perfects import broad_na, is_numlike

def precision_recall(df):
    """
    TP: true positive
    FP: false positive (we report extra value)
    FN: false negative (we miss a value)
    wrong: predicted positive, but kcat and km are wrong
    """
    # define psuedo-precision as TP / (TP + FP) = TP / PP
    # which is a measure of how good our data is
    # define psuedo-recall as TP / (TP + FN)
    # which is a measure of how much data we can collect

    # predicted positive: since we know that brenda doesn't always have all the data

    # predicted positive: whenever we find something which has an analog in brenda
    PP = df.filter(
        (pl.col('kcat').is_not_null() | pl.col('km').is_not_null())
        & (pl.col('kcat_2').is_not_null() | pl.col('km_2').is_not_null())
    )

    # true positive: among those, how many are actually correct
    TP = PP.filter(
        pl.col('kcat_feedback').is_null() & pl.col('km_feedback').is_null()
    )

    wrong = PP.filter(
        pl.col('kcat_feedback').is_not_null() | pl.col('km_feedback').is_not_null()
    )

    # FP = PP.filter(
    #     pl.col('kcat_feedback').is_not_null() | pl.col('km_feedback').is_not_null()
    # )
    FP = df.filter(
        (pl.col('kcat').is_not_null() | pl.col('km').is_not_null())
        & (pl.col('kcat_2').is_null() & pl.col('km_2').is_null())
    )


    FN = df.filter(
        (pl.col('kcat').is_null() & pl.col('km').is_null())
        & (pl.col('kcat_2').is_not_null() | pl.col('km_2').is_not_null())
    )

    return TP, FP, FN, wrong

def mean_log_relative_ratio(df: pl.DataFrame, col_name: str) -> float:
    """
    Calculate mean log of the percentage error between col_name and col_name_2
    (calculate mean order of magnitude difference)
    
    Args:
        df: Polars DataFrame
        col_name: Base column name. Will compare with {col_name}_2
    
    Returns:
        float: Mean absolute percentage error
    """
    # Create expression to parse both columns
    def parse_col(col: str) -> pl.Expr:
        def to_true(x):
            value, unit, _ = parse_value_and_unit(x)
            return convert_to_true_value(value, unit)
        return pl.col(col).map_elements(to_true, return_dtype=pl.Float64)
    
    # Calculate MAPE using Polars expressions

    # hmm = df.with_columns([
    #         parse_col(col_name).alias('val_a'),
    #         parse_col(f'{col_name}_2').alias('val_b')
    #     ]).filter(
    #         (pl.col('val_a').is_not_null()) & 
    #         (pl.col('val_b').is_not_null()) & 
    #         (pl.col('val_a') != 0) & 
    #         (pl.col('val_b') != 0)
    #     )
    
    result = (
        df
        # .filter(~pl.col(col_name).str.contains(r"\^"))  # Exclude rows with ^
        .with_columns([
            parse_col(col_name).alias('val_a'),
            parse_col(f'{col_name}_2').alias('val_b'),
            # pl.col(col_name)
        ])
        .filter(
            (pl.col('val_a').is_not_null()) & 
            # (pl.col('val_a').
            (pl.col('val_b').is_not_null()) &
            (pl.col('val_a') < 1e9) & # oops, there were some outliers
            (pl.col('val_b') < 1e9) & # oops, there were some outliers
            (pl.col('val_a') != 0) &
            (pl.col('val_b') != 0)  # Avoid division by zero
        )
        .select(
            # pl.col('val_a'),
            (pl.col('val_a') / pl.col('val_b'))
            .log(base=10)
            .abs()
        )
        .mean()
        .item()
    )

    # now get the percent where the values are within 10% of each other
    # within_10 = (
    #     hmm
    #     .filter(
    #         ((pl.col('val_a') - pl.col('val_b')).abs() / pl.max_horizontal(['val_a', 'val_b'])) < 0.1
    #     )
    #     .height / hmm.height
    # )
    # print(f"Within 10% for {col_name}: {within_10}")
    
    return result if result is not None else 0.0
    
def string_similarity(df: pl.DataFrame, col_name: str) -> float:
    """
    Calculate mean string similarity ratio between col_name and col_name_2 columns
    using RapidFuzz's ratio function
    
    Args:
        df: Polars DataFrame
        col_name: Base column name. Will compare with {col_name}_2
        
    Returns:
        float: Mean similarity ratio (0-1 where 1 means identical strings)
    """
    from rapidfuzz import fuzz

    col_name_2 = f"{col_name}_2"
    
    result = (
        df
        .filter(
            (pl.col(col_name).is_not_null()) & 
            (pl.col(col_name_2).is_not_null())
        )
        .select(
            pl.struct([col_name, col_name_2])
            .map_elements(lambda x: fuzz.ratio(str(x[col_name]), str(x[col_name_2])) / 100.0, return_dtype=pl.Float64)
            .alias('similarity')
        )
        .mean()
        .item()
    )
    
    return result if result is not None else 1.0

# Alternative version using Jaccard similarity of character bigrams
# This version doesn't require additional dependencies
def string_similarity_jaccard(df: pl.DataFrame) -> float:
    """
    Calculate mean Jaccard similarity of character bigrams between kcat and kcat_2
    
    Args:
        df: Polars DataFrame with kcat and kcat_2 columns
    
    Returns:
        float: Mean similarity ratio (0-1 where 1 means identical strings)
    """
    def get_bigrams(s: str) -> set:
        """Convert string to set of character bigrams"""
        s = str(s)
        return set(s[i:i+2] for i in range(len(s)-1))
    
    def jaccard_sim(x: dict) -> float:
        """Calculate Jaccard similarity between two strings"""
        s1, s2 = str(x['kcat']), str(x['kcat_2'])
        bigrams1 = get_bigrams(s1)
        bigrams2 = get_bigrams(s2)
        
        if not bigrams1 and not bigrams2:
            return 1.0
        
        intersection = len(bigrams1 & bigrams2)
        union = len(bigrams1 | bigrams2)
        
        return intersection / union if union > 0 else 0.0
    
    result = (
        df
        .filter(
            (pl.col('kcat').is_not_null()) & 
            (pl.col('kcat_2').is_not_null()) &
            (~pl.col('kcat').str.contains(r"\^"))  # Exclude rows with ^
        )
        .select(
            pl.struct(['kcat', 'kcat_2'])
            .map_elements(jaccard_sim)
            .alias('similarity')
        )
        .mean()
        .item()
    )
    
    return result if result is not None else 1.0


def get_accuracy_score(df: pl.DataFrame, allow_brenda_missing: bool = True) -> tuple[int, int]:
    """
    Calculate agreement score for kinetic parameters between columns and their _2 counterparts.
    
    Args:
        df: Polars DataFrame with km, kcat, kcat_km columns and their _2 variants
        allow_brenda_missing: If True, allows BRENDA values to be missing
        
    Returns:
        tuple: (agreement count, total count)
    """
        # Convert the broad_na and is_numlike functions to work with polars
    broad_na_expr = lambda col: pl.col(col).map_elements(broad_na, return_dtype=pl.Boolean, skip_nulls=False)
    is_numlike_expr = lambda col: pl.col(col).map_elements(is_numlike, return_dtype=pl.Boolean, skip_nulls=False)
    
    # First, filter for rows where at least one BRENDA value exists
    df_filtered = df.filter(
        ~(
            broad_na_expr('km_2') & 
            broad_na_expr('kcat_2') & 
            broad_na_expr('kcat_km_2')
        )
    )
    
    # Calculate total count
    total = len(df_filtered)
    
    # Add computed columns for null checks and numeric checks
    with_checks = (
        df_filtered
        .with_columns([
            # Check if values are numlike
            is_numlike_expr('km').alias('km_numlike'),
            is_numlike_expr('km_2').alias('km_2_numlike'),
            is_numlike_expr('kcat').alias('kcat_numlike'),
            is_numlike_expr('kcat_2').alias('kcat_2_numlike'),
            is_numlike_expr('kcat_km').alias('kcat_km_numlike'),
            is_numlike_expr('kcat_km_2').alias('kcat_km_2_numlike'),
            
            # Check if feedback is empty
            broad_na_expr('km_feedback').alias('km_feedback_empty'),
            broad_na_expr('kcat_feedback').alias('kcat_feedback_empty'),
            
            # Check if main values are empty
            broad_na_expr('km').alias('km_empty'),
            broad_na_expr('kcat').alias('kcat_empty'),
            broad_na_expr('kcat_km').alias('kcat_km_empty'),
        ])
    )
    
    # Calculate agreement count
    agreement = (
        with_checks
        .filter(
            (
                # km must match in numlike-ness
                (pl.col('km_numlike') == pl.col('km_2_numlike'))
                &
                # kcat must match in numlike-ness (or allow brenda missing)
                (
                    (pl.col('kcat_numlike') == pl.col('kcat_2_numlike')) |
                    (allow_brenda_missing & ~pl.col('kcat_2_numlike'))
                )
                &
                # kcat_km must match in numlike-ness (or allow brenda missing)
                (
                    (pl.col('kcat_km_numlike') == pl.col('kcat_km_2_numlike')) |
                    (allow_brenda_missing & ~pl.col('kcat_km_2_numlike'))
                )
                &
                # at least one value must not be empty
                ~(
                    pl.col('km_empty') & 
                    pl.col('kcat_empty') & 
                    pl.col('kcat_km_empty')
                )
                &
                # feedback must be empty
                pl.col('km_feedback_empty') &
                pl.col('kcat_feedback_empty')
            )
        )
        .height
    )
    
    return agreement, total