import pandas as pd
import numpy as np
from scipy.optimize import linear_sum_assignment
from difflib import SequenceMatcher
import re

from enzyextract.hungarian.postmatched_utils import left_shift_pmid

from enzyextract.hungarian.csv_fix import courtesy_fix_pmids, lengthen_enzyme_name



def assign_default_units(a_unit, b_unit, value_name):
    if a_unit is None: 
        if value_name == 'kcat':
            a_unit = "s^-1"
        elif value_name == 'km':
            a_unit = "mM"
    if b_unit is None:
        if value_name == 'kcat':
            b_unit = "s^-1"
        elif value_name == 'km':
            b_unit = "mM"
    return a_unit, b_unit
def are_synonyms(a, b):
    # Placeholder function, replace with actual implementation
    # return 1 if a == b else 0
    
    return string_similarity(a, b)



def calc_sigfigs(mantissa: str):
    # remove nonnumeric
    mantissa = ''.join(filter(str.isdigit, mantissa))
    # remove leading zeros
    # consider 100 to have 3 sigfigs
    return len(mantissa.lstrip('0')) 
    
# NB: order matters
valid_units = ["ms^-1", "sec^-1", "s^-1", "min^-1", "m^-1", "hr^-1", "h^-1", "mM", "µM", "nM", "M"]
def parse_value_and_unit(value_str):
    # match = re.match(r"(\d+(?:\.\d+)?)\s*(\S+)", value_str)
    # if match:
    #     return float(match.group(1)), match.group(2)
    exponent_factor = 1
    # ·
    if "10^" in value_str or "x" in value_str or '×' in value_str: # \u00d7
        # exponent
        exponent = re.search(r"10\^(-?\d+)", value_str)
        if not exponent:
            exponent = re.search(r"[x×]\s*10(-?\d+)", value_str)
        if exponent:
            exponent_factor = 10 ** int(exponent.group(1))
        
        # now, truncate the exponent out of the value_str
        # that way, we don't parse the exponent as a mantissa - for example, 10^-4
        exp_idx = exponent.start() if exponent else len(value_str)
        mantissa_part = value_str[:exp_idx] # + value_str[exponent.end():]
    elif "e" in value_str:
        # scientific notation
        exponent = re.search(r"\de(-?\d+)", value_str, re.IGNORECASE)
        if exponent:
            exponent_factor = 10 ** int(exponent.group(1))
        
        # now, truncate the exponent out of the value_str
        # oops, the regex includes the digit before e, so need to add 1
        exp_idx = exponent.start()+1 if exponent else len(value_str)
        mantissa_part = value_str[:exp_idx]
    else:
        mantissa_part = value_str
    
    numeric_splitter = min(mantissa_part.find("±"), mantissa_part.find(" -- ")) # brenda supports ranges
    numeric_part = mantissa_part[:numeric_splitter] if numeric_splitter != -1 else mantissa_part
    match = re.match(r"(\d+(?:\.\d+)?)[\s±]*", numeric_part)
    if match:
        value = float(match.group(1))
    else:
        if mantissa_part == '':
            # The input string is like 10^-4: no mantissa
            value = 1
        else:
            return None, None, None
    
    # 
    if value_str.endswith("mol L^-1"):
        value_str = value_str[:-7] + "M"
    
    # last step: detect unit
    for unit in valid_units:
        if unit in value_str:
            return value * exponent_factor, unit, None # calc_sigfigs(match.group(1))

    time_canon = {'sec': 's', 'h': 'hr'}
    for time_unit in ['s', 'sec', 'min', 'h', 'hr']:
        canonic = time_canon.get(time_unit, time_unit)
        if value_str.endswith(f"/{time_unit}"):
            return value * exponent_factor, canonic + '^-1', None
        elif value_str.endswith(f" per {time_unit}"):
            return value * exponent_factor, canonic + '^-1', None

    # print("Unknown unit:", value_str)
    return value * exponent_factor, None, None
    

def convert_to_true_value(value, unit, sigfigs=None):
    kcat_conversions = {
        "ms^-1": 1000,
        "s^-1": 1,
        "sec^-1": 1,
        "min^-1": 1/60,
        "m^-1": 1/60,
        "hr^-1": 1/3600,
        "h^-1": 1/3600
    }
    km_conversions = {
        "M": 1,
        "mM": 1e-3,
        "µM": 1e-6,
        "nM": 1e-9
    }
    
    if unit in kcat_conversions:
        return value * kcat_conversions[unit]
    elif unit in km_conversions:
        return value * km_conversions[unit]
    return value

    
    
def float_similarity(a, b):
    """
    Range: [0, 1]
    This is really just relative error, but inverted
    """
    if a == 0 and b == 0:
        return 1
    return 1 - abs(a - b) / max(a, b)
    # return 1 / (1 + abs(a - b) / max(a, b))

def mislabeled_unit_similarity(a_mantissa, b_mantissa, max_score=0.95):
    """
    When the mantissa is extremely close, and only differ by a mislabeled unit
    ie. 10 s^-1 versus 10.1 min^-1
    
    Only consider if mantissas are extremely similar. Otherwise, return 0.
    For instance, for 10 s^-1, anything further than 10.2 s^-1 is too far away.
    
    :param max_score float: The maximum similarity score to return
    """
    similarity = float_similarity(a_mantissa, b_mantissa)
    if similarity > 0.99: 
        # only consider if mantissa is practically identical
        # ie. about 2 sigfigs in common
        return max_score
    return 0

def off_by_10_similarity(a_value, b_value, 
                         off_by_10_score=0.7, off_by_100_score=0.5, 
                         off_by_1000_score=0.9, off_by_more_score=0.3, 
                         instead_return_ratio=False,
                         base=10):
    """
    For when the annotator accidentally converted wrong by a factor of 10.
    This isn't a perfect match, but it should be worth more than the base similarity of 0.526315
    
    Off by 1000 is actually a common error, so it should be placed pretty high.
    """
    if a_value == 0 or b_value == 0:
        return 0
    
    ratio = max(a_value, b_value) / min(a_value, b_value)
    eps = 1e-6 # floating point error
    if instead_return_ratio:
        off_by_10_score = base**1 # 10
        off_by_100_score = base**2 # 100
        off_by_1000_score = base**3 # 1000
    if abs(ratio - base**1) < eps: # 10
        return off_by_10_score
    elif abs(ratio - base**2) < eps: # 100
        return off_by_100_score
    elif abs(ratio - base**3) < eps: # 1000
        return off_by_1000_score
    else:
        # if ratio is very close to 10**n, return off_by_more_score
        n = round(np.log10(ratio))
        if n <= 0:
            return 0 # do not accept off by 1
        if abs(ratio - base**n) < eps: # 10**n
            if instead_return_ratio:
                return base**n
            return off_by_more_score
    return 0

def value_similarity(a, b, value_name='kcat'):
    a_na = pd.isna(a)
    b_na = pd.isna(b)
    if a_na and b_na:
        return 1
    if a_na or b_na:
        return 0
        
    
    
    a_mantissa, a_unit, a_sigfigs = parse_value_and_unit(a)
    b_mantissa, b_unit, b_sigfigs = parse_value_and_unit(b)
    
    a_unit, b_unit = assign_default_units(a_unit, b_unit, value_name)
    
    if a_mantissa is None or b_mantissa is None:
        return 0
    
    a_value = convert_to_true_value(a_mantissa, a_unit)
    b_value = convert_to_true_value(b_mantissa, b_unit)
    
    # true value similarity
    value_similarity = float_similarity(a_value, b_value)
    
    if value_similarity != 1:
        value_similarity *= 0.9 # penalize imperfect matches
    
    # similarity just among sigfigs. relevant if units simply labeled wrong
    unit_similarity = mislabeled_unit_similarity(a_mantissa, b_mantissa)
    
    off_by_10_score = off_by_10_similarity(a_value, b_value) if value_name == 'km' else \
        off_by_10_similarity(a_value, b_value, base=60) # if 'kcat', then watch for off-by-60 errors. Note that previous matching had 0.

    
    return max(value_similarity, unit_similarity, off_by_10_score)

def km_similarity(a, b):
    return value_similarity(a, b, value_name='km')

def mutant_similarity(a, b):
    # if both wildtype, not both empty: 0.5
    # if mutant: 1
    
    a_here = (not pd.isna(a)) and a # because of ''
    b_here = (not pd.isna(b)) and b
    
    if a_here and b_here:
        # if both not empty
        if a.lower() == b.lower():
            # ensure it is a mutant format
            if re.match(r"\b[A-Z]\d+[A-Z]\b", a): 
                return 1
        if is_wildtype(a, allow_empty=False) and is_wildtype(b, allow_empty=False):
            return 1 # both wild-type: good.
    
    # allow matching empty to "wild-type", but don't reward it as much
    if (a_here or b_here) and is_wildtype(a) and is_wildtype(b):
        return 0.2
    
    # if both not empty
    
    return 0

def is_wildtype(mutant, allow_empty=True):
    if pd.isna(mutant) or not mutant:
        return allow_empty
    if mutant.lower() in ["wt", "wildtype", "wild type", "wild-type"]:
        return True
    return False
def string_similarity(a, b):
    if pd.isna(a) or pd.isna(b):
        return 0
    ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
    # clip to 0 if too little
    # if ratio >= 0.5:
        # return ratio
    return ratio

def calculate_similarity_matrix(df1, df2, coefficients):
    n, m = len(df1), len(df2)
    similarity_matrix = np.zeros((n, m))
    
    for i in range(n):
        for j in range(m):
            similarity = 0
            for col, coeff in coefficients.items():
                # if col == "enzyme":
                #     similarity += coeff * are_synonyms(df1.loc[i, col], df2.loc[j, col])
                assert col in df1.columns and col in df2.columns, f"Column {col} not found in options {df1.columns} vs {df2.columns}"
                if col == "substrate": # TODO are_synonyms
                    similarity += coeff * string_similarity(df1.loc[i, col], df2.loc[j, col])
                elif col == "mutant":
                    similarity += coeff * mutant_similarity(df1.loc[i, col], df2.loc[j, col])
                
                # elif col == "variant":
                    # similarity += coeff * string_similarity(df1.loc[i, col], df2.loc[j, col])
                elif col == "kcat":
                    similarity += coeff * value_similarity(df1.loc[i, col], df2.loc[j, col], value_name='kcat')
                elif col == "km":
                    similarity += coeff * value_similarity(df1.loc[i, col], df2.loc[j, col], value_name='km')
            # similarity += kcat_similarity(df1.loc[i, "kcat"], df2.loc[j, "kcat"], value_coeff=coefficients["kcat_value"], unit_coeff=coefficients["kcat_unit"])
            # similarity += km_similarity(df1.loc[i, "km"], df2.loc[j, "km"], value_coeff=coefficients["km_value"], unit_coeff=coefficients["km_unit"])
            
            similarity_matrix[i, j] = similarity
    
    return similarity_matrix

def hungarian_matching(df1, df2, coefficients):
    similarity_matrix = calculate_similarity_matrix(df1, df2, coefficients)
    
    # Pad the similarity matrix if the dataframes have different sizes
    n, m = similarity_matrix.shape
    if n > m:
        padding = np.zeros((n, n - m))
        similarity_matrix = np.hstack((similarity_matrix, padding))
    elif m > n:
        padding = np.zeros((m - n, m))
        similarity_matrix = np.vstack((similarity_matrix, padding))
    
    row_ind, col_ind = linear_sum_assignment(similarity_matrix, maximize=True)
    
    matches = []
    for i, j in zip(row_ind, col_ind):
        if i < len(df1):
            if j < len(df2): 
                matches.append((df1.index[i], df2.index[j], similarity_matrix[i, j]))
            else:
                matches.append((df1.index[i], None, similarity_matrix[i, j]))
            # use None for padding
        else:
            matches.append((None, df2.index[j], similarity_matrix[i, j]))
            # use None for padding
            
    
    return matches

def join_dataframes_with_matching(df1, df2, matches):
    # Create a mapping of df2 indices to df1 indices
    # index_map = {match[1]: match[0] for match in matches}
    
    # assume that the doi are all the same
    
    
    # Rename columns in df2 to avoid conflicts
    df2 = df2.rename(columns={col: f"{col}_2" for col in df2.columns})
    
    # Reindex df2 to match df1's index
    # df2_reindexed = df2_renamed.reindex(index=[index_map.get(i, i) for i in range(len(df2))])
    
    # Join the dataframes
    # joined_df = pd.concat([df1, df2_reindexed], axis=1)
    
    # Sort the dataframe by df1's index
    # joined_df = joined_df.sort_index()
    
    joined_rows = []
    
    # Iterate over the list of tuples
    for (i, j, score) in matches:
        # Select the i-th row from DataFrame A and the j-th row from DataFrame B
        # but i or j could be None. In that case, create a row of NaNs
        if i is not None:
            row_A = df1.loc[i]
        else:
            row_A = pd.Series([''] * len(df1.columns), index=df1.columns)
        if j is not None:
            row_B = df2.loc[j]
            # set doi
        else:
            row_B = pd.Series([''] * len(df2.columns), index=df2.columns)
        
        # Concatenate the rows horizontally (axis=0)
        joined_row = pd.concat([row_A, row_B], axis=0)
        
        # Append the joined row to the list
        joined_rows.append(joined_row)
    
    # Create a new DataFrame from the list of joined rows
    # reindex too
    joined_df = pd.DataFrame(joined_rows, dtype=str)
    joined_df = joined_df.reset_index(drop=True)
    
    
    return joined_df

def clean_df(df: pd.DataFrame, brenda=False):
    # perform some cleaning
    # replace mu (μ) with micro (µ)
    df["kcat"] = df["kcat"].str.replace("μ", "µ")
    df["km"] = df["km"].str.replace("μ", "µ")
    
    # replace S^-1 with s^-1
    df["kcat"] = df["kcat"].str.replace("S^-1", "s^-1")
    df["kcat"] = df["kcat"].str.replace("sec^-1", "s^-1")
    df["km"] = df["km"].str.replace("S^-1", "s^-1")
    
    # remove commas in numbers
    df["kcat"] = df["kcat"].str.replace(",", "")
    df["km"] = df["km"].str.replace(",", "")
    
    # lowercase the doi
    df["doi"] = df["doi"].str.lower()
    if brenda:
        # append s^-1 to kcat values
        df["kcat"] = df["kcat"] + " s^-1"
        df["km"] = df["km"] + " mM"
    
    df.dropna(subset=["kcat", "km"], how='all', inplace=True)
    df.reset_index(drop=True, inplace=True)
    # df.fillna("", inplace=True)
    return df
    
def feedback_for_match(a, b, col_name):
    """
    Evaluate the quality of a match
    """
    if pd.isna(a) or pd.isna(b):
        return ''
    a_mantissa, a_unit, a_sigfigs = parse_value_and_unit(a)
    b_mantissa, b_unit, b_sigfigs = parse_value_and_unit(b)
    
    feedback = []

    
    if a_mantissa is None or b_mantissa is None:
        return ''
    
    if a_unit is None:
        feedback.append(f"unknown unit: {a}")
    if b_unit is None:
        feedback.append(f"unknown unit: {b}")
    a_value, b_value = assign_default_units(a_unit, b_unit, col_name)
    
    a_value = convert_to_true_value(a_mantissa, a_unit)
    b_value = convert_to_true_value(b_mantissa, b_unit)
    
    # similarity just among sigfigs. relevant if units simply labeled wrong
    
    value_similarity = float_similarity(a_value, b_value)
    
    misunit_similarity = mislabeled_unit_similarity(a_mantissa, b_mantissa)
    
    # off by factor
    if col_name == 'km':
        off_by_factor = off_by_10_similarity(a_value, b_value,
            instead_return_ratio=True)
        if off_by_factor:
            feedback.append(f"off by {off_by_factor}")
    # wrong unit
    elif a_unit != b_unit and misunit_similarity > value_similarity:
        if feedback:
            feedback += ", "
        feedback.append(f"wrong unit")
    
    if not feedback and value_similarity < 0.98:
        feedback.append(f"value deviation {a_value:.5g} vs {b_value:.5g}")
    return ', '.join(feedback)
    
def match_dfs_by_pmid(df1, df2, pmids=None, coefficients=None):
    if coefficients is None:
        coefficients = {
            "enzyme": 0.3,
            "variant": 0.1,
            "substrate": 0.2, # 0.2
            "kcat": 0.8,
            "km": 0.5,
        }
    
    # rename doi to pmid if needed
    if "pmid" not in df1.columns:
        df1 = df1.rename(columns={"doi": "pmid"})
    if "pmid" not in df2.columns:
        df2 = df2.rename(columns={"doi": "pmid"})
    # out of courtesy, detect if a column is int-type while the other is str-type
    
    
    if pmids is None:
        # get unique pmids in df1 and df2
        pmids = df1["pmid"].unique()
        if not pmids:
            pmids = df2["pmid"].unique()
    if not pmids:
        raise ValueError("No pmids given")
    
    # TODO properly make this
    # courtesy check that the type is correct
    pmids = courtesy_fix_pmids(pmids, df1, df2)
            # 
            

    
    aggregated_df = pd.DataFrame()
    for pmid in pmids:
        
        df1_subset = df1[df1["pmid"] == pmid].reset_index(drop=True) # .astype(str)
        
        df2_subset = df2[df2["pmid"] == pmid].reset_index(drop=True) # .astype(str)
    
        
        matches = hungarian_matching(df1_subset, df2_subset, coefficients)
        joined_df = join_dataframes_with_matching(df1_subset, df2_subset, matches)
        
        # joined_df.drop(columns=["pmid_2"], inplace=True)
        
        # add feedback
        # first, fill with empty strings
        joined_df["km_feedback"] = ""
        joined_df["kcat_feedback"] = ""
        for i, j, score in matches:
            if i is not None and j is not None:
                for col in ["km", "kcat"]:
                    feedback = feedback_for_match(df1_subset.loc[i, col], df2_subset.loc[j, col], col)
                    joined_df.loc[i, f"{col}_feedback"] = feedback
                    # joined_df.loc[j, f"{col}_feedback"] = feedback
        
        # yield joined_df
        aggregated_df = pd.concat([aggregated_df, joined_df])
    aggregated_df = left_shift_pmid(aggregated_df)
    return aggregated_df
        
def convert_to_eval_format(joined_df):
    """
    Convert the joined dataframe to the evaluation format
    """
    joined_df = joined_df.copy()
    if 'enzyme_full' in joined_df.columns:
        # lambda: if len(enzyme) < 10, use f"enzyme_full ({enzyme})"
        joined_df['enzyme'] = joined_df.apply(lambda x: lengthen_enzyme_name(x['enzyme'], x['enzyme_full']), axis=1)
    if 'enzyme_full_2' in joined_df.columns:
        joined_df['enzyme_2'] = joined_df.apply(lambda x: lengthen_enzyme_name(x['enzyme_2'], x['enzyme_full_2']), axis=1)
    
    subset = joined_df[["pmid", "enzyme", "enzyme_2", "substrate", "substrate_2", "variant", "variant_2"]]
    # rename columns
    subset = subset.rename(columns={
        "enzyme": "enzyme_a",
        "enzyme_2": "enzyme_b",
        "substrate": "substrate_a",
        "substrate_2": "substrate_b",
        "variant": "variant_a",
        "variant_2": "variant_b"
    })
    # split into one df per pmid
    
    # return pd.DataFrame(rows)
    # return list(subset.groupby("pmid"))
    # return subset
    return subset

if __name__ == "__main__":
    df1 = clean_df(pd.read_csv('examples/eval/brenda_50_gt.tsv', sep='\t', dtype=str))
    df2 = clean_df(pd.read_csv('examples/eval/brenda_50_brenda.tsv', sep='\t', dtype=str), brenda=True)
    joined_df = match_dfs_by_pmid(df1, df2)
        # display(df)
    joined_df['pmid'] = joined_df['pmid'].replace('', np.nan)
    joined_df['pmid'] = joined_df['pmid'].combine_first(joined_df[f"pmid_2"])
    joined_df.drop(columns=[f"pmid_2"], inplace=True)
    # replace commas with semicolons everywhere, for purposes of csv
    joined_df = joined_df.map(lambda x: x.replace(",", ";") if isinstance(x, str) else x)
    joined_df.to_csv("brenda_50_hungarian_matched.tsv", sep="\t", index=False)

