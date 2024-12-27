import re

from Bio.Data.IUPACData import protein_letters_3to1_extended as amino3to1

amino1 = "ACDEFGHIKLMNPQRSTVWY"
amino3 = "Ala|Cys|Asp|Glu|Phe|Gly|His|Ile|Lys|Leu|Met|Asn|Pro|Gln|Arg|Ser|Thr|Val|Trp|Tyr"
mutant_pattern = re.compile(r'\b([ACDEFGHIKLMNPQRSTVWY][1-9]\d{1,3}[ACDEFGHIKLMNPQRSTVWY])\b')
mutant_v2_pattern = re.compile(rf'\b(?:{amino3})[1-9]\d{{1,3}}(?:{amino3})\b', re.IGNORECASE)
mutant_v3_pattern = re.compile(rf'\b((?:{amino3})[1-9]\d{{0,3}}(?:{amino3}))\b', re.IGNORECASE)

mutant_v4_pattern = re.compile(rf'\b((?:{amino3})-?[1-9]\d{{1,3}})\b', re.IGNORECASE)

# mutant_v5_pattern = re.compile(rf"\b((?:{amino3})-?\d{{1,4}}(?:\s*→\s*|\s*to\s*|\s*>\s*|!)-?(?:{amino3}))\b") # if arrow or "to", then it is unambiguously a point mutation.
mutant_v5_pattern = re.compile(rf"\b((?:{amino3})-?\d{{1,4}}(?:\s*→\s*|\s*to\s*|\s*>\s*|!)-?(?:{amino3}))\b")

standardize_mutants1_re = re.compile(rf"({amino3})-?(\d{{1,4}})(\s?→\s?| to |\s?>\s?|!)[ -]?({amino3})(?:[^\-\w\d]|$)") # if arrow or "to", then it is unambiguously a point mutation.

import polars as pl
import polars.selectors as cs

def with_clean_mutants(df: pl.DataFrame):
    """produces a df with clean mutants

    Required columns:
    - mutant (str)

    Temporary columns generated:
    - mutant1
    - mutant3

    Adds column:
    - clean_mutant (list of str)
    """
    df = df.with_columns([
        pl.col("mutant").str.replace_all(standardize_mutants1_re.pattern, r"$1$2$4").alias("mutant")
    ])
    df = df.with_columns([
        pl.col("mutant").str.extract_all(mutant_pattern.pattern).alias("mutant1"),
        pl.col("mutant").str.extract_all(mutant_v3_pattern.pattern).alias("mutant3"),
    ])

    df = df.with_columns([
        pl.col("mutant3").list.eval(pl.element().str.replace_many(amino3to1)).alias("mutant3"),
    ]).with_columns([
        pl.col("mutant1").list.concat(pl.col("mutant3")).alias("clean_mutant")
    ]).select(cs.exclude('mutant1', 'mutant3'))
    return df