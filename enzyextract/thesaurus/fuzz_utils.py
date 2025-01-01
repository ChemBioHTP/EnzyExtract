from enzyextract.utils.pl_utils import wrap_pbar
from rapidfuzz import fuzz

from tqdm import tqdm
import polars as pl

def calculate_similarity(x, col1, col2, check_shorter=True):
    lhs = x[col1].lower()
    rhs = x[col2].lower()
    if check_shorter and (not rhs or len(rhs) <= len(lhs)):
        return None  # Skip if rhs is shorter than or equal in length to lhs
    return fuzz.partial_ratio(lhs, rhs)  # Returns similarity score (0-100)

def compute_fuzz_with_progress(df, comparisons):
    """
    Comparison: tuple of (col1, col2, condition_col, similarity_column)

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

            df = df.with_columns([
                pl.struct(
                    pl.col(col1),
                    pl.col(col2)
                ).map_elements(
                    wrap_pbar(pbar, lambda x: calculate_similarity(x, col1, col2, check_shorter=condition_col)),
                    return_dtype=pl.Float64
                ).alias(similarity_column)
            ])

    return df
