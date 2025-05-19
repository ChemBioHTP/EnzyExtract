import polars as pl

r_hyphens = r'[-᠆‑‒–—―﹘﹣－˗−‐⁻]'
r_micros = r'[µμ]' # NOTE: 2 different characters! prefer 'micro'
# "µM": 1e-6, # micro sign, u+00b5
# "μM": 1e-6, # mu sign, u+03bc

def unicode_fix(plcol: pl.Expr) -> pl.Expr:
    if isinstance(plcol, str):
        plcol = pl.col(plcol)
    return (
        plcol.str.replace_all(r_hyphens, "-")
        .str.replace_all(r_micros, "µ")
        # .str.replace_all(r'\s+', ' ') # standardize whitespace
        # .str.strip_chars() # remove leading/trailing whitespace
        # .str.replace_all(r"[\u200B-\u200D\uFEFF]", "") # remove zero-width characters
    )
def unicode_fix_list(plcol: pl.Expr) -> pl.Expr:
    if isinstance(plcol, str):
        plcol = pl.col(plcol)
    return (
        plcol.list.eval(
            unicode_fix(pl.element())
        )
    )

r_unclassified = r"(?i)^purified( enzyme)?|soluble|free( enzyme)?|control|average|In cells|in vitro|in vivo$"

r_recombinant = r"(?i)mutant|recombinant"

r_mutant_many_1to4_amino1_legacy = r"\b([A-Z]\d{1,4}[A-Z](\/[A-Z]\d{1,4}[A-Z])*)\b"
# from enzyextract.thesaurus.mutant_patterns import mutant_pattern as r_mutant_1
r_mutant_many_2to4_amino1_legacy = r"\b([A-Z]\d{2,4}[A-Z](\/[A-Z]\d{2,4}[A-Z])*)\b"
# r_mutant = r"([Mm]utant )?{r_mutant_2to4}( [Mm]utant)?"

import enzyextract.thesaurus.mutant_patterns
r_mutant_single_2to4_amino1 = enzyextract.thesaurus.mutant_patterns.mutant_pattern.pattern
r_mutant_single_1to4_amino3 = enzyextract.thesaurus.mutant_patterns.mutant_v3_pattern.pattern
r_mutant_single_1to4_amino3plus = enzyextract.thesaurus.mutant_patterns.standardize_mutants1_re.pattern

def _to_many(r: str):
    """convert a regex to a many regex"""
    r_no_b = r.removeprefix(r'\b').removesuffix(r'\b')
    return r'\b{r_no_b}((\/| ?[\-\+] ?|[ ]){r_no_b})*\b'.replace(r'{r_no_b}', r_no_b)
r_mutant_many_2to4_amino1 = _to_many(r_mutant_single_2to4_amino1)
r_mutant_many_1to4_amino3 = _to_many(r_mutant_single_1to4_amino3)
r_mutant_many_1to4_amino3plus = _to_many(r_mutant_single_1to4_amino3plus)
# r_mutant_omni = rf'([Mm]utant |[Vv]ariant )?({r_mutant_many_2to4_amino1}|{r_mutant_many_1to4_amino3}|{r_mutant_many_1to4_amino3plus})( [Mm]utant| [Vv]ariant)?'
r_mutant_omni = (
    # r'( |^) +' # optionally enforce a very strict word boundary
    r'([Mm]utant |[Vv]ariant )?'
    + rf'({r_mutant_many_2to4_amino1}|{r_mutant_many_1to4_amino3}|{r_mutant_many_1to4_amino3plus})' 
    + r'( [Mm]utant| [Vv]ariant)?'
    # + r'( |$)' # optionally enforce a very strict word boundary

)

# see: enzyextract.thesaurus.mutant_patterns
# see: standardize_mutants1_re
r_wildtype = r"(?i)\bwild[\- ]?type?\b"
r_wt = r"\bWT\b"

r_wt_exact = r"(?i)^(wild[\- ]?type?|wt)$"
r_wt_inexact = r"(?i)(wild[\- ]?type?|wt)"

r_pH = r"pH (\b\d+(?:\.\d+)?\b)"
# r'^pH( =)? \d+(\.\d+)?( ?- ?\d+(\.\d+)?)?$'

# NOTE: it's important that this regex is an EXACT match, solely because the 
# '\/' character can be both a separator (ie. between pH and temperature) and
# a range separator (ie. between two pH values). So if this is not an exact match, 
# a string could be parsed incorrectly. So if you wish to use this regex
# as a substring-filter-out regex, remove the '\/' character.
r_pH_range = (
    r'^(optimal )?pH( ?[=~><] ?| )' # "pH = "
    r'\d+(\.\d+)?' # "7.5"
    r'(( ?[±\/\-] ?| to )\d+(\.\d+)?)?$' # "± 0.5" (optional range or error)
)

# r_temp_exact = r'^-?\d+(\.\d+)?°C$'
r_temp = r"\b(\d+(?:\.\d+)?) ?°C\b"

# NOTE: see note for r_pH_range.
r_temp_range = (
    r'^(T ?|(optimal )?temperature )?[=~><]? ?' # "T = " (optional)
    r'[+\-]?\d+(\.\d+)?' # "-43.5"
    r'(( ?°C)?( ?[±\/\-] ?| to )-?\d+(\.\d+)?)' # "°C to 100" (optional range or error)
    r'? ?°C$' # "°C"
)
r_temp_kelvin = r_temp_range.replace('°C', 'K')

# r'(-?\d+(\.\d+)?°C)'
r_temp_range_lite = r'\b' + r_temp_range[1:-1] + r'\b'

# https://www.leonschools.net/cms/lib/FL01903265/Centricity/Domain/4929/polyatomic%20ion%20ref%20sheet.pdf
r_ions= r'\b((Li|Na|K|Rb|Cs|Ag|Cu)[+⁺]|(Mg|Ca|Sr|Ba|Zn|Cd|Cr|Mn|Fe|Co|Sn|Pb|Hg)(2+|²⁺)|Mn(2+|⁴+)|Cr(2+|³+)|Pb(2+|⁴+))|((F|Cl|Br|I)[-]|(NO3|SO4)(2-|²-)|PO4(3-|³-))\b'


__all__ = [
    "r_hyphens",
    "r_micros",
    "unicode_fix",
    "unicode_fix_list",
    "r_unclassified",
    "r_recombinant",
    "r_mutant_many_1to4_amino1_legacy",
    "r_mutant_many_2to4_amino1_legacy",
    "r_mutant_single_2to4_amino1",
    "r_mutant_single_1to4_amino3",
    "r_mutant_single_1to4_amino3plus",
    "r_mutant_many_2to4_amino1",
    "r_mutant_many_1to4_amino3",
    "r_mutant_many_1to4_amino3plus",
    "r_mutant_omni",
    # "r_mutant_2",
    "r_wildtype",
    "r_wt",
    "r_wt_exact",
    "r_wt_inexact",
    "r_pH",
    "r_pH_range",
    "r_temp",
    "r_temp_range",
    "r_temp_range_lite",
    "r_temp_kelvin",
    "r_ions",
]

# Dynamically collect all variables starting with "r_"
_additional = [name for name in locals() if name.startswith("r_") and name not in __all__]
# Add other non-regex functions that should be exported
__all__.extend(_additional)