"""
Instead of matching tuples of (enzyme, substrate, kcat, km, mutant) in {A, B} to each other, 
this evaluation uses a relaxed version where kcat and km are no longer associatedx. 
Hence, the unordered set of kcat are matched from A to B, without regard to the enzyme, substrate, or mutant.

This is because BRNEDA does not always provide tuples (kcat, km) due to variation in comments.
"""

import math
import polars as pl

from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit

def mantissa_exponent_similarity(a, b, alpha=0.9, beta=0.2, base=10):
    
    # Scale mantissas to [0, 1) and exponents to base-10
    e_a = int(math.log(a, base)) if a != 0 else 0
    e_b = int(math.log(b, base)) if b != 0 else 0

    m_a = a / base**e_a if a != 0 else 0
    m_b = b / base**e_b if b != 0 else 0
    
    # Compute similarity score
    similarity = 1 / (1 + alpha * abs(m_a - m_b) + beta * abs(e_a - e_b))
    
    return similarity

def _off1000_favoritism(e_diff):
    """
    give the exponent distance, with discounts when things are off by 1000 or 1000000

    1: 2
    2: 2
    3: 0.5
    4: 4
    5: 5
    6: 1.25
    7: 7
    8: 8
    9: 9
    """
    if e_diff == 1:
        return 2
    elif e_diff == 3:
        return 0.5
    elif e_diff == 6:
        return 1.25
    return e_diff


def biased_mantissa_exponent_similarity(a, b, alpha=0.9, beta=0.2, base=10, exponent_treatment=_off1000_favoritism):
    """
    hack: because 

    bias the weight of exponent difference so that x1000 and x1000000 are more favored
    """
    e_a = round(math.log(a, base)) if a != 0 else 0
    e_b = round(math.log(b, base)) if b != 0 else 0

    m_a = a / base**e_a if a != 0 else 0
    m_b = b / base**e_b if b != 0 else 0

    # eps = 1e-6 # floating point error

    e_diff = abs(e_a - e_b)

    # give preference to certain exponent differences
    e_error = exponent_treatment(e_diff)
    # if abs(m_a - m_b) < eps and e_diff == 3:
        # return 0.9

    

    similarity = 1 / (1 + alpha * abs(m_a - m_b) + beta * e_error)
    return similarity

import numpy as np
from scipy.optimize import linear_sum_assignment
def find_optimal_number_matching(set1: list[int], set2: list[int], 
                               similarity=biased_mantissa_exponent_similarity) -> list[tuple[int, int]]:
    """
    Find the optimal matching between two sets of numbers that maximizes
    the total similarity between paired numbers using mantissa-exponent similarity.
    
    Args:
        set1: First set of numbers
        set2: Second set of numbers
        alpha: Weight for mantissa difference (default 0.9)
        beta: Weight for exponent difference (default 0.2)
        
    Returns:
        List of tuples containing the matched pairs (number from set1, number from set2)
    """
    # Create similarity matrix
    similarity_matrix = np.zeros((len(set1), len(set2)))
    for i, n1 in enumerate(set1):
        for j, n2 in enumerate(set2):
            similarity_matrix[i, j] = similarity(n1, n2)
    
    # Negate similarity matrix for linear_sum_assignment (it minimizes by default)
    # This effectively turns maximization into minimization
    cost_matrix = -similarity_matrix
    
    # Use Hungarian algorithm to find optimal matching
    row_ind, col_ind = linear_sum_assignment(cost_matrix)
    
    # Create pairs from the matching
    matches = []
    # used_set2_indices = set(col_ind)
    
    # Handle matched pairs
    for i, j in zip(row_ind, col_ind):
        matches.append((set1[i], set2[j], similarity_matrix[i, j]))
    
    # Handle unmatched numbers from set1
    unmatched_set1 = [(i, n) for i, n in enumerate(set1) if i not in row_ind]
    for _, n in unmatched_set1:
        matches.append((n, None, 0.0))  # Unmatched pairs have similarity of 0
    
    # Handle unmatched numbers from set2
    unmatched_set2 = [(j, n) for j, n in enumerate(set2) if j not in col_ind]
    for _, n in unmatched_set2:
        matches.append((None, n, 0.0))  # Unmatched pairs have similarity of 0
        
    # Sort by the first number in each pair
    # matches.sort(key=lambda x: x[0] if x[0] is not None else x[1])
    
    return matches

def match_by_unique_numeric(df1: pl.DataFrame, df2: pl.DataFrame, col_names: list[str], pmid_col='pmid') -> pl.DataFrame:
    """
    Match two dataframes by unique numeric values in the specified columns.
    """

    common_pmids = df1.filter(pl.col(pmid_col).is_in(df2[pmid_col]))[pmid_col].unique().to_list()

    result = []
    # repeat for each PMID:
    # get the optimal matching of kcats
    # get the optimal matching of kms
    # get the optimal matching of mutants
    from tqdm import tqdm
    for pmid in tqdm(common_pmids):
        df1_pmid = df1.filter(pl.col(pmid_col) == pmid)
        df2_pmid = df2.filter(pl.col(pmid_col) == pmid)

        for col_name in col_names:
            set1 = df1_pmid[col_name].to_list()
            set2 = df2_pmid[col_name].to_list()

            result1 = []
            result2 = []

            for s in set1:
                if s is None:
                    continue
                value, unit, _ = parse_value_and_unit(s)
                if value is None:
                    continue
                result1.append(convert_to_true_value(value, unit))
            for s in set2:
                if s is None:
                    continue
                value, unit, _ = parse_value_and_unit(s)
                if value is None:
                    continue
                result2.append(convert_to_true_value(value, unit))

            result1 = list(set(result1))
            result2 = list(set(result2))

            matches = find_optimal_number_matching(result1, result2)
            # columns: pmid, col_name, set1, set2, similarity
            df = pl.DataFrame({
                'pmid': [pmid] * len(matches),
                'col_name': [col_name] * len(matches),
                'set1': [m[0] for m in matches],
                'set2': [m[1] for m in matches],
                'similarity': [m[2] for m in matches]
            }, schema_overrides={'pmid': pl.Utf8, 'col_name': pl.Utf8, 'set1': pl.Float64, 'set2': pl.Float64, 'similarity': pl.Float64})
            result.append(df)
    
    return pl.concat(result)

def km_similarity(a, b):
    return biased_mantissa_exponent_similarity(a, b)

def off60_favoritism(e_diff):
    return e_diff

def kcat_similarity(a, b):
    return biased_mantissa_exponent_similarity(a, b, base=60, exponent_treatment=off60_favoritism)

def match_by_unique_kcat_km(df1: pl.DataFrame, df2: pl.DataFrame, pmid_col='pmid') -> pl.DataFrame:
    """
    Match two dataframes by kcat and km.
    """

    common_pmids = df1.filter(pl.col(pmid_col).is_in(df2[pmid_col]))[pmid_col].unique().to_list()

    builder = []
    # repeat for each PMID:
    # get the optimal matching of kcats
    # get the optimal matching of kms
    # get the optimal matching of mutants
    from tqdm import tqdm


    for pmid in tqdm(common_pmids):
        df1_pmid = df1.filter(pl.col(pmid_col) == pmid)
        df2_pmid = df2.filter(pl.col(pmid_col) == pmid)

        src = None
        if 'src' in df1_pmid.columns:
            src = df1_pmid['src'][0]
        if 'src' in df2_pmid.columns:
            src = df2_pmid['src'][0]

        for col_name in ['kcat', 'km']:
            set1 = df1_pmid[col_name].to_list()
            set2 = df2_pmid[col_name].to_list()

            set1vals = {}
            set2vals = {}

            for set_, dict_ in [(set1, set1vals), (set2, set2vals)]: 
                for s in set_:
                    if s is None:
                        continue
                    value, unit, _ = parse_value_and_unit(s)
                    if value is None:
                        continue
                    dict_[convert_to_true_value(value, unit)] = s

            if col_name == 'kcat':
                similarity = kcat_similarity
            else:
                similarity = km_similarity

            matches = find_optimal_number_matching(list(set1vals.keys()), list(set2vals.keys()))
            # columns: pmid, col_name, set1, set2, similarity

            df_dict = {
                'pmid': [],
                'col_name': [],
                'orig1': [],
                'orig2': [],
                'set1': [],
                'set2': [],
                'similarity': [],
                'src': []
            }
            for m in matches:
                df_dict['pmid'].append(pmid)
                df_dict['col_name'].append(col_name)
                df_dict['orig1'].append(set1vals.get(m[0], None))
                df_dict['set1'].append(m[0])
                df_dict['set2'].append(m[1])
                df_dict['orig2'].append(set2vals.get(m[1], None))
                df_dict['similarity'].append(m[2])
                df_dict['src'].append(src)

            df = pl.DataFrame(df_dict, schema_overrides={
                'pmid': pl.Utf8, 'col_name': pl.Utf8,'orig1': pl.Utf8, 'orig2': pl.Utf8, 'set1': pl.Float64, 'set2': pl.Float64, 'similarity': pl.Float64, 'src': pl.Utf8})
            builder.append(df)
    
    df = pl.concat(builder)

    df = df.with_columns([
        pl.when(
            pl.col('set1').is_null() | pl.col('set2').is_null()
        ).then(None).otherwise(
            pl.max_horizontal(['set1', 'set2']) / 
            pl.min_horizontal(['set1', 'set2'])
        ).alias('ratio')
    ])
    return df


if __name__ == '__main__':
    # Example usage
    print(biased_mantissa_exponent_similarity(1.2, 1.2))
    print(biased_mantissa_exponent_similarity(1.2, 12))
    print(biased_mantissa_exponent_similarity(1.2, 120))
    print(biased_mantissa_exponent_similarity(1.2, 1200))  # Should be higher
    print(biased_mantissa_exponent_similarity(1.2, 12000))  # Should be higher
    print(biased_mantissa_exponent_similarity(1.2, 120000))  # Should be higher
    print(biased_mantissa_exponent_similarity(1.2, 1200000))  # Should be higher
    print()
    
    print(biased_mantissa_exponent_similarity(5, 5.1))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 51))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 510))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 5100))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 51000))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 510000))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 5100000))     # Should be lower
    print()
    print(biased_mantissa_exponent_similarity(1.2, 5))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 6))     # Should be lower
    print(biased_mantissa_exponent_similarity(5, 600000))     # Should be lower
    print()
    print(biased_mantissa_exponent_similarity(9, 1000))     # Should be lower
    print(biased_mantissa_exponent_similarity(10, 1000))     # Should be lower
    print()
    print(biased_mantissa_exponent_similarity(0.9, 1))
    print(biased_mantissa_exponent_similarity(1, 1))
    print(biased_mantissa_exponent_similarity(1.1, 1))
    print(biased_mantissa_exponent_similarity(0.9, 10))
    print(biased_mantissa_exponent_similarity(1, 10))
    print(biased_mantissa_exponent_similarity(1.1, 10))

    for i in range(1, 1000):
        assert biased_mantissa_exponent_similarity(i, i + 0.1) > 0.9, f"Failed for {i}"
        assert biased_mantissa_exponent_similarity(i, i - 0.1) > 0.9, f"Failed for {i}"
    
    # Example usage
    set2 = [1, 2, 6, 7, 8, 9, 10]
    set1 = [1, 3, 7, 8, 1000]

    result = find_optimal_number_matching(set1, set2)

    # Print results with similarity scores
    for n1, n2, sim in result:
        if n2 is None:
            print(f"{n1} (unmatched)")
        else:
            print(f"{n1} to {n2} (similarity: {sim:.3f})")
