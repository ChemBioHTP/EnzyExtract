from difflib import SequenceMatcher
import re


def is_wildtype(mutant, allow_empty=True):
    if mutant is None or not mutant:  # pd.isna(mutant) 
        return allow_empty
    if mutant.lower() in ["wt", "wildtype", "wild type", "wild-type"]:
        return True
    return False
def string_similarity(a, b):
    # if pd.isna(a) or pd.isna(b):
    if a is None or b is None:
        return 0
    ratio = SequenceMatcher(None, a.lower(), b.lower()).ratio()
    # clip to 0 if too little
    # if ratio >= 0.5:
        # return ratio
    return ratio


# NB: order matters
valid_units = ["ms^-1", "sec^-1", "s^-1", "min^-1", "m^-1", "hr^-1", "h^-1", "mM", 
               "µM", # micro
               "μM", # mu
               "nM", "M"]
def parse_value_and_unit(value_str):
    # match = re.match(r"(\d+(?:\.\d+)?)\s*(\S+)", value_str)
    # if match:
    #     return float(match.group(1)), match.group(2)
    value_str = value_str.replace("μ", "µ") # mu -> micro
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
        "ms^-1": 1000.0,
        "s^-1": 1.0,
        "sec^-1": 1.0,
        "min^-1": 1/60,
        "m^-1": 1/60,
        "hr^-1": 1/3600,
        "h^-1": 1/3600
    }
    km_conversions = {
        "M": 1.0,
        "mM": 1e-3,
        "µM": 1e-6, # micro sign, u+00b5
        "μM": 1e-6, # mu sign, u+03bc
        "nM": 1e-9
    }
    
    if unit in kcat_conversions:
        return value * kcat_conversions[unit]
    elif unit in km_conversions:
        return value * km_conversions[unit]
    if isinstance(value, int):
        return float(value)
    return value