from enzyextract.utils.pl_utils import wrap_pbar
from rapidfuzz import fuzz

from tqdm import tqdm
import polars as pl

def calculate_similarity(x, col1, col2, case_sensitive=False):
    lhs = x[col1]
    rhs = x[col2]
    if lhs is None or rhs is None:
        return None
    if not case_sensitive:
        lhs = lhs.lower()
        rhs = rhs.lower()

    # if check_shorter and (not rhs or len(rhs) <= len(lhs)):
        # return None  # Skip if rhs is shorter than or equal in length to lhs
    return fuzz.partial_ratio(lhs, rhs)  # Returns similarity score (0-100)

def calculate_similarity_list(x, col1, col2, case_sensitive=False):
    """
    For when col2 is a list
    """
    if not case_sensitive:
        lhs = x[col1].lower()
    else:
        lhs = x[col1]
    rhs = x[col2]

    if not rhs:
        return None  # Skip if rhs is empty

    if not case_sensitive:
        return max(fuzz.partial_ratio(lhs, y.lower()) for y in rhs)  # Returns similarity score (0-100)
    else:
        return max(fuzz.partial_ratio(lhs, y) for y in rhs)

def compute_fuzz_with_progress(df: pl.DataFrame, comparisons):
    """
    Comparison: tuple of (col1, col2, case_sensitive, similarity_column)

    col1: pl.Utf8
    col2: pl.Utf8 OR pl.List(pl.Utf8)

    Example:
    comparisons = [
        ('enzyme_preferred', 'enzyme_name', False, 'similarity_enzyme_name'),
        ('organism_fixed', 'organism_right', False, 'similarity_organism'),
        ('organism', 'organism_common', False, 'similarity_organism_common'),
    ]
    
    """
    total_comparisons = df.height * len(comparisons)
    
    with tqdm(total=total_comparisons) as pbar:
        for col1, col2, condition_col, similarity_column in comparisons:
            if similarity_column is None:
                similarity_column = f"similarity_{col1}_vs_{col2}"

            if isinstance(df.schema[col2], pl.List):
                lambdar = lambda x: calculate_similarity_list(x, col1, col2, case_sensitive=condition_col)
            else:
                lambdar = lambda x: calculate_similarity(x, col1, col2, case_sensitive=condition_col)
            df = df.with_columns([
                pl.struct(
                    pl.col(col1),
                    pl.col(col2)
                ).map_elements(
                    wrap_pbar(pbar, lambdar),
                    return_dtype=pl.Float64
                ).alias(similarity_column)
            ])

    return df
