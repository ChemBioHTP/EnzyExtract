
import re

import pandas as pd

_strange_kcat_units = set()
_strange_km_units = set()

def fix_km(x: str) -> str:
    # match \bmm\b
    if pd.isna(x):
        return None
    if not isinstance(x, str):
        x = str(x)
    
    x = re.sub(',', '', x) # destroy commas, which interfere with matching
    x = re.sub(r'\bmm\b', 'mM', x)
    x = re.sub(r'\bnm\b', 'nM', x)
    x = re.sub(r'\b[puμµ]M\b', 'µM', x, flags=re.IGNORECASE) # greek mu \u03BC --> \u00B5
    # micro symbol \u00B5

    x = re.sub(r'mol/L\b', 'M', x, flags=re.IGNORECASE) # also works if mol has a prefix
    
    units = ''.join(letter for letter in x if letter.isalpha() and not letter in ['x', 'X']) # allow the cross symbol
    acceptable_units = ['M', 'mM', 'µM', 'nM', 'pM']
    if not units:
        pass
    elif units not in acceptable_units:
        if 'g/' in x:
            # print("Rejecting Km with g/L unit", x)
            return None
        elif 'mg' in x:
            # print("Rejecting Km with mg unit", x)
            return None
        # print(f"Strange km unit: {x}")
        _strange_km_units.add(units)
        return x
    
    return x

def fix_kcat(x: str) -> str:
    
    if pd.isna(x):
        return None
    if not isinstance(x, str):
        x = str(x)
    
    x = re.sub(',', '', x) # destroy commas, which interfere with matching
    x = re.sub('μ', 'µ', x) # greek mu \u03BC --> \u00B5
    # detect and destroy units
    units = ''.join(letter for letter in x if letter.isalpha() and not letter == 'x') # allow the cross symbol
    
    acceptable_units = ['ms', 'millisecond', 's', 'sec', 'second', 'm', 'min', 'minute', 'h', 'hr', 'hour', 'day']
    bad_units = ['mol', 'mg', 'U', '/g', 'l/', 'L']
    more_bad_units = [
        re.compile(x) for x in [r'\bM\b']
    ] + [
        re.compile(unit, re.IGNORECASE) for unit in [r'\bmM\b', r'\bpM\b', r'\bµM\b', r'\bnM\b']
    ]
    
    if not units:
        # no unit. allow for now
        pass
    elif not any(letter.isdigit() for letter in x):
        # no number at all. allow for now
        # this is probably a null value
        pass
    elif units not in acceptable_units: 
        if any(unit in x for unit in bad_units) or any(unit.search(x) for unit in more_bad_units):
            return None
        # strange-looking unacceptable unit
        # print(f"Strange kcat unit: {x}")
        _strange_kcat_units.add(units)
        # return ''
    
    # standardize hyphens
    x = x.replace('−', '-').replace('–', '-') # minus (\u2212), then en dash (\u2013)
    x = x.replace("-'", "-1")
    x = re.sub(r'\bsec\b', 's', x)
    for unit in acceptable_units:
        x = re.sub(r'\b' + f'{unit}' + r'-1\b', unit + '^-1', x)
    return x
    

def lengthen_enzyme_name(short: str, longer: str) -> str:
    if pd.isna(short):
        return longer
    if pd.isna(longer):
        return short
    
    if short == longer:
        return short
    if len(short) >= 10:
        return short
    return f"{longer} ({short})"
    

def widen_df(df: pd.DataFrame, brenda=True) -> pd.DataFrame:
    # various widening of brenda data
    def extract_mutant(comment: str, brenda=False):
        if not pd.isna(comment):
            if not brenda or "mutant" in comment or "recombinant" in comment:
                # require "mutant" or "recombinant" keyword for brenda only
                mutants = re.findall(r"\b[A-Z]\d{2,4}[A-Z]\b", comment)
                out = '/'.join(mutants)
                if out:
                    return out
            # look for WT
            wildtype = re.findall(r"\bwild[\- ]?type?\b", comment, flags=re.IGNORECASE)
            if wildtype:
                return 'wild-type'
            wildtype = re.findall(r"\bWT\b", comment)
            if wildtype:
                return 'WT'
        return pd.NA
    def extract_mutant_brenda(comment: str):
        return extract_mutant(comment, brenda=True)

    def extract_pH(comment: str):
        if not pd.isna(comment) and "pH" in comment:
            # pH (\b\d+\.?\d+\b)
            pH = re.findall(r"pH (\b\d+(?:\.\d+)?\b)", comment)
            if pH:
                return pH[0]
        return pd.NA
    def extract_temp(comment: str):
        if not pd.isna(comment) and "°C" in comment:
            temp = re.findall(r"\b(\d+(?:\.\d+)?) ?°C\b", comment)
            if temp:
                return temp[0]
        return pd.NA
    
    target = 'comments'
    if 'comments' not in df.columns:
        target = 'variant'
        if 'variant' not in df.columns:
            target = 'descriptor'
    df = df.copy()
    
    def use_new_values(df, name, col):
        if name in df.columns:
            df[name] = df[name].fillna(col)
        else:
            df[name] = col
    if brenda:
        use_new_values(df, 'mutant', df[target].apply(extract_mutant_brenda))
    else:
        use_new_values(df, 'mutant', df[target].apply(extract_mutant))
    use_new_values(df, 'pH', df[target].apply(extract_pH))
    use_new_values(df, 'temperature', df[target].apply(extract_temp))
    return df

def prep_for_hungarian(df: pd.DataFrame, printme=True) -> pd.DataFrame:
    if 'turnover_number' in df.columns:
        # brenda variant
        # must add mM suffix to km_value
        # df = df.drop(columns=['kcat_km'])
        # cast km_value and turnover_number to str
        
        with pd.option_context('mode.chained_assignment', None):
            df['km_value'] = df['km_value'].apply(lambda x: str(x) + ' mM' if not pd.isna(x) else None)
            df['turnover_number'] = df['turnover_number'].apply(lambda x: str(x) + ' s^-1' if not pd.isna(x) else None)
        df.rename(columns={'turnover_number': 'kcat', 'km_value': 'km', 'comments': 'variant'}, inplace=True)
    else:
        # regular variant
        
        # df = df.loc[:, ["doi", "enzyme","variant","substrate","kcat","km"]] # ,"kcat/km"]]
        df.rename(columns={'doi': 'pmid'}, inplace=True)
        if 'comments' in df.columns and 'variant' not in df.columns:
            # could be a reformed brenda variant
            df.rename(columns={'comments': 'variant'}, inplace=True)
        df['km'] = df['km'].apply(fix_km)
        df['kcat'] = df['kcat'].apply(fix_kcat)
        if printme:
            print("Strange kcat units", _strange_kcat_units)
            print("Strange km units", _strange_km_units)
        

    # cast pmid column to int, then str
    if pd.api.types.is_integer_dtype(df['pmid']):
        df['pmid'] = df['pmid'].astype(str)
    assert 'pmid' in df.columns
    

    return df

# def prep_for_hungarian(df: pd.DataFrame) -> pd.DataFrame:
#     df.dropna(subset=["kcat", "km"], how='all', inplace=True)
#     # df.reset_index(drop=True, inplace=True)
#     df.fillna("", inplace=True)
#     return df
def _int_like(some_type):
    return 'int' in str(some_type)

def courtesy_fix_pmids(pmids, df, df2=None):
    """Make sure that pmids are the same type as the df"""
    
    if not pmids:
        return []
    
    if df2 is not None:
        if _int_like(df2["pmid"].dtype) != _int_like(df["pmid"].dtype):
            print("[!!!] WARNING: df1 pmids are", df["pmid"].dtype, "but df2 pmids are", df2["pmid"].dtype, "(likely strings)")
            print("Converting both to strings.")
            # convert both to strings
            df.astype({'pmid': 'str'}, inplace=True)
            df2.astype({'pmid': 'str'}, inplace=True)
    
    given_type = type(list(pmids)[0])
    df_type = df["pmid"].dtype
    if _int_like(given_type) != _int_like(df_type):
        print("[!!!] WARNING: provided pmids are", given_type, "but df1 pmids are", df_type, "(likely strings)")
        if _int_like(given_type):
            print("Converting pmids to str")
            pmids = [str(pmid) for pmid in pmids]
        else:
            print("Converting pmids to int")
            pmids = [int(pmid) for pmid in pmids]
    return pmids