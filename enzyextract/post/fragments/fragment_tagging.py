"""
see also:
enzyextract.hungarian.csv_fix.pl_widen_df
"""


import polars as pl

# from enzyextract.post.regexes import r_hyphens, r_mutant_2to4, r_pH_range, r_recombinant, r_temp_kelvin, r_temp_range, r_unclassified, r_wt_exact, r_wt_inexact, unicode_fix
from enzyextract.post.regexes import *
from enzyextract.thesaurus.organism_patterns import organism_patterns
from enzyextract.thesaurus.uniprot_organisms import load_uniprot_names

datadf = pl.read_parquet('data/recontext/1_fromyaml/data.parquet')
datadf = datadf.filter(
    pl.col('kcat').is_not_null() | pl.col('km').is_not_null() | pl.col('kcat_km').is_not_null()
).with_columns(
    unicode_fix('descriptor'),
    unicode_fix_list('fragments')
)
contextdf = pl.read_parquet('data/recontext/1_fromyaml/context.parquet').with_row_index('context_id')
# keep exactly one custom_id (keep the last: keep the final answer)
contextdf = contextdf.unique('custom_id', keep='last')
# standardize all context
# contextdf = contextdf.with_columns(
#     unicode_fix_list('temperatures'),
#     unicode_fix_list('pHs'),
# )

descriptors = datadf.select('custom_id', 'descriptor', 'fragments').unique().with_row_index('data_id')

descriptors = descriptors.join(
    contextdf.select('context_id', 'custom_id'), # each custom_id gets exactly one context_id
    left_on='custom_id',
    right_on='custom_id',
    how='inner',
    validate='m:1'
)
fragments = descriptors.explode('fragments').with_row_index('fragment_id')
remf = fragments.clone() # remaining fragments
# 531_459

def filter_out(regex: str, remf: pl.DataFrame, *, on='fragments', extract=False) -> pl.DataFrame:
    """
    Filter a DataFrame by a regex pattern.
    """
    mask = remf.select(
        pl.col(on).str.contains(regex)
    ).fill_null(False).to_series()
    subset = remf.filter(mask)
    remf = remf.filter(~mask)

    if extract:
        subset = subset.with_columns(
            pl.col(on).str.extract(regex, 0).alias('extract')
        ) # .drop('shrinkable', strict=False)
    return subset, remf


def subfilter_out(regex: str, remf: pl.DataFrame, drop_shrinkable=True) -> pl.DataFrame:
    """
    Filter a DataFrame by a regex pattern, and extract substrings
    """
    assert not regex.endswith('$')

    # subset = remf.filter(remf['shrinkable'].str.contains(regex))

    # remove additional punctuation
    regex_out = rf"\(?{regex}\)?(, | ?\/ ?)?"

    subset = remf.with_columns(
        pl.col('shrinkable').str.extract(regex, 0).alias('extract'),
        # pl.col('shrinkable').str.replace(regex_out, '').alias('rest')
    ).drop_nulls('extract').drop('fragment_lower', strict=False)
    if drop_shrinkable:
        subset = subset.drop('shrinkable')
    remf = remf.with_columns(
        pl.col('shrinkable')
        .str.replace(regex_out, '')
        # .str.strip_suffix(', ')
        .alias('shrinkable')
    ).filter(
        pl.col('shrinkable').str.len_chars() > 0 # remove empty strings
    )
    return subset, remf

def join_out(rhs: pl.DataFrame, remf: pl.DataFrame, right_on: str, **kwargs) -> pl.DataFrame:
    subset = remf.join(
        rhs,
        left_on=['context_id', 'fragments'],
        right_on=['context_id', right_on],
        how='inner',
        **kwargs
    ).drop('shrinkable', strict=False).drop('fragment_lower', strict=False)
    # remf = remf.filter(~pl.col('fragment_id').is_in(set(subset['fragment_id'])))
    remf = remf.join(
        subset,
        on='fragment_id',
        how='anti',
    )
    return subset, remf

def join_out_lower(rhs: pl.DataFrame, remf: pl.DataFrame, right_on: str, **kwargs) -> pl.DataFrame:
    subset = remf.join(
        rhs,
        left_on=['context_id', 'fragment_lower'],
        right_on=['context_id', right_on],
        how='inner',
        **kwargs
    ).drop('fragment_lower')
    # remf = remf.filter(~pl.col('fragment_id').is_in(set(subset['fragment_id'])))
    remf = remf.join(
        subset,
        on='fragment_id',
        how='anti',
    )
    return subset, remf

def substring_by_column(df: pl.DataFrame, haystack_col: str, needle_col: str) -> pl.DataFrame:
    df = df.with_columns(
        pl.when(pl.col(needle_col).is_not_null()).then(
            pl.col(haystack_col)
            # .str.replace(pl.col('extract'), '', literal=True)
            # https://github.com/pola-rs/polars/issues/14367
            .str.replace(pl.col(needle_col).first(), '', literal=True).over(needle_col)
            .str.replace(r', (, |$)', '') # remove extra commas
            .str.replace(r' \(\)', '') # remove extra parentheses
            .str.strip_chars()
            .alias(haystack_col)
        ).otherwise(pl.col(haystack_col))
    ).filter(
        pl.col(haystack_col).str.len_chars() > 0 # remove empty strings
    )
    return df

def subjoin_out(rhs: pl.DataFrame, remf: pl.DataFrame, right_on: str, *, remf_on='shrinkable', drop_shrinkable=True, **kwargs) -> pl.DataFrame:
    """
    Extracts substrings (described by rhs) from remf. Uses the 'shrinkable' column.
    So rhs should contain shorter strings.
    
    If multiple matches are found, the first one is used. 
    (A recommendation: sort rhs by decreasing length, so the longest substring is used.)

    Outputs in 'extract' column.
    """
    product = remf.join(
        rhs,
        left_on=['context_id'],
        right_on=['context_id'],
        how='inner',
        **kwargs
    )
    subset = product.filter(
        pl.col(remf_on).str.contains(pl.col(right_on), literal=True)
    ).rename({
        right_on: 'extract'
    }) # .drop(right_on)
    successful_matches = subset.select(
        'fragment_id', 
        'extract'
    ).unique('fragment_id', keep='first')
    successful_remf = remf.join(successful_matches, on='fragment_id', validate='m:1', how='inner', coalesce=True)
    rest_remf = remf.join(successful_matches, on='fragment_id', how='anti')

    # successful_remf = successful_remf.with_columns(
    #     pl.when(pl.col('extract').is_not_null()).then(
    #         pl.col('shrinkable')
    #         # .str.replace(pl.col('extract'), '', literal=True)
    #         # https://github.com/pola-rs/polars/issues/14367
    #         .str.replace(pl.col('extract').first(), '', literal=True).over('extract')
    #         .str.replace(r', (, |$)', '') # remove extra commas
    #         .str.strip_chars()
    #         .alias('shrinkable')
    #     ).otherwise(pl.col('shrinkable'))
    # ).filter(
    #     pl.col('shrinkable').str.len_chars() > 0 # remove empty strings
    # )
    successful_remf = substring_by_column(successful_remf, remf_on, 'extract')
    successful_remf.drop_in_place('extract')
    remf = rest_remf.merge_sorted(successful_remf, key='fragment_id')
    subset = subset.drop('fragment_lower', strict=False)
    if drop_shrinkable:
        subset = subset.drop(remf_on)
    return subset, remf

def pick_longest_string(plcol: pl.Expr) -> pl.Expr:
    """
    From a column of list of strings, pick the longest string.
    If list is empty, return null.
    """
    return plcol.list.get(
        plcol.list.eval(
            pl.element().str.len_chars().arg_max()
        ).list.get(0, null_on_oob=True) # get the index of the longest string
    ) # get it from the list

def filter_list_out(many: list[str], remf: pl.DataFrame, ascii_case_insensitive=False) -> pl.DataFrame:
    """
    Filter a DataFrame by a list of strings.
    TODO: what doesn't work is respecting word boundaries.
    """

    # temporarily add space to shrinkable, as a form of word boundary respecting
    remf = remf.with_columns(
        (' ' + pl.col('shrinkable') + ' ').alias('shrinkable')
    )
    subset = remf.with_columns(
        pick_longest_string(
            pl.col('shrinkable').str.extract_many(many, overlapping=True, ascii_case_insensitive=ascii_case_insensitive)
        ).alias('extract')
    ).drop('literal', strict=False)
    rest_remf = subset.filter(
        pl.col('extract').is_null()
    )
    subset = subset.filter(
        pl.col('extract').is_not_null()
    )

    successful_remf = substring_by_column(subset, 'shrinkable', 'extract')
    remf = rest_remf.merge_sorted(successful_remf, key='fragment_id')
    remf = remf.with_columns(
        pl.col('shrinkable').str.strip_chars() # remove space added to shrinkable
    )
    remf = remf.drop('extract')
    return subset, remf

### STEP 1.1: pH
## 1.1a) exact pH matches
pH_exact, remf = filter_out(r_pH_range, remf)
pH_exact2, remf = filter_out(r_pH_suffixed, remf)
# print(pH_exact) # 28_231

### STEP 1.2: temperature
## 1.2a) exact temperature matches

temp_exact, remf = filter_out(r_temp_range, remf)
# kelvin is delayed to after step 4 (enzymes)
# print(temp_exact) # 23_372 -> 23_761




### STEP 1.3: cofactors (tags that begin with 'with')

cofactors, remf = filter_out(r'^\(?[Ww]ith[o ]', remf)
# print(cofactors) # 17_798

### STEP 1.4: enzyme names
enzymedf = contextdf.select('context_id', 'enzymes').explode('enzymes').unnest('enzymes').sort('context_id').with_row_index('enzyme_id')
# enzymedf = enzymedf.with_columns(
#     unicode_fix(pl.col('fullname')),
#     unicode_fix_list('synonyms'),
#     unicode_fix_list('organisms'),
#     unicode_fix_list('mutants'),
# )
enzymedf_synonyms = enzymedf.select('context_id', 'enzyme_id', 'synonyms').explode('synonyms')

## 1.4a) exact fullname matches
enzyme_exact_full, remf = join_out(
    enzymedf.select('context_id', 'enzyme_id', 'fullname'),
    remf,
    'fullname',
    # validate='m:1'
)
# print(enzyme_exact_full) # 43_906

## 1.4b) exact synonym matches
enzyme_exact_syn, remf = join_out(
    enzymedf_synonyms,
    remf,
    'synonyms',
    # validate='m:1'
)
# print(enzyme_exact_syn) # 90_470

### STEP 1.5: mutants
## 1.5a) "wild-type"
wt_exact, remf = filter_out(r_wt_exact, remf)

## 5b. mutants verbatim from the context
mutantsdf = enzymedf.select('context_id', 'enzyme_id', 'mutants').explode('mutants')
# remove WT
mutantsdf = mutantsdf.filter(
    ~pl.col('mutants').str.contains(r_wt_exact)
)
mutants_exact_ctxl, remf = join_out(
    mutantsdf,
    remf,
    'mutants',
) # 62_941


### STEP 1.6: organisms
# NOTE: for organisms, it is common for fragments to be matched multiple times!
# Possible duplication:
# a. multiple enzymes share the same organism (multiple enzyme_id)
# b. a short genus name is ambiguous (ie. Laminaria japonica vs Lampetra japonica: see organisms_sg_ambiguous)

organismdf = enzymedf.select('context_id', 'enzyme_id', 'organisms').explode('organisms')

# convert, for instance, "Escherichia coli" to "E. coli"
# (short genus)
organisms_sg = organismdf.filter(
    pl.col('organisms').str.contains(r'^([A-Za-z])([a-z]*) (.*)') # needs at least 2 words
).with_columns(
    pl.col('organisms').str.replace(r'^([A-Za-z])([a-z]*) (.*)', r'$1. $3').alias('organism_sg')
) # .drop_nulls('organism_sg')

organisms_sg_ambiguous = organisms_sg.select('organisms', 'organism_sg').unique()
organisms_sg_ambiguous = organisms_sg_ambiguous.filter(
    organisms_sg_ambiguous.select('organism_sg').is_duplicated()
)

## 1.6a) "expressed in" organisms aren't where the enzyme is actually from
organisms_expressedin, remf = filter_out(r'(?i)expressed in', remf) # 765


## 1.6b) exact organism matches
organisms_exact, remf = join_out(
    organismdf,
    remf,
    'organisms',
) # 16_544


## 1.6c) exact organism matches (short genus, ctxl, connected to enzyme id)
organisms_exact_sg_ctxl, remf = join_out(
    organisms_sg,
    remf,
    'organism_sg',
) # 2_913 -> 2_867 + 348

# these matches kind of suck: they have some false positives and/or also remove some 
# additional information
# hence, do not remove them from the remaining fragments

## 1.5d) inexact mention of "mutant" or "recombinant"
# removing from fragments leaves the risk of removing enzyme or organism information
# remf = remf.filter(~pl.col('fragment_id').is_in(set(recombinant_inexact['fragment_id'])))

## 1.5d) [RETRO] regex for point mutants
# single-digit mutants are risking, because it would loop in false positives like "H2O" or "D2O"
# 2+ digits are safer
mutants_exact_re, remf = filter_out(
    # rf"^([Mm]utant )?{r_mutant_many_2to4_amino1_legacy}( [Mm]utant)?$",
    r'^' + r_mutant_omni + r'$',
    remf
) # 3_339 -> 3_769

### STEP 1.7: extra substrates
substratedf = (
    contextdf.select('context_id', 'substrates')
    .explode('substrates').unnest('substrates')
    .sort('context_id').with_row_index('substrate_id')
)
# substratedf = substratedf.with_columns(
#     unicode_fix(pl.col('fullname')),
#     unicode_fix_list('synonyms'),
# )
subs_exact_full, remf = join_out(
    substratedf.select('context_id', 'substrate_id', 'fullname'),
    remf,
    'fullname',
) # 9_107
subs_exact_syn, remf = join_out(
    substratedf.select('context_id', 'substrate_id', 'synonyms').explode('synonyms'),
    remf,
    'synonyms',
) # 5_505

### STEP 1.8: kelvin (needs to be done after enzymes)
temp_exact_k, remf = filter_out(r_temp_kelvin, remf)

### STEP 1.9: activity type
activity_type, _ = filter_out(r' activity$', remf)

### STEP 1.99: filter out known unclassified fragments
unclassified_known, remf = filter_out(r_unclassified, remf)

##### STEP 2.x: case insensitive


remf = remf.with_columns(
    pl.col('fragments').str.to_lowercase().alias('fragment_lower'),
)
enzymedfi = enzymedf.with_columns(
    pl.col('fullname').str.to_lowercase().alias('fullname_lower'),
    pl.col('synonyms').list.eval(pl.element().str.to_lowercase()).alias('synonyms_lower'),
)
substratedfi = substratedf.with_columns(
    pl.col('fullname').str.to_lowercase().alias('fullname_lower'),
    pl.col('synonyms').list.eval(pl.element().str.to_lowercase()).alias('synonyms_lower'),
)
### 2.4: enzymes

## 2.4a) case-insensitive enzyme fullname matches
enzyme_casei_full, remf = join_out_lower(
    enzymedfi.select('context_id', 'enzyme_id', 'fullname_lower'),
    remf,
    'fullname_lower',
    # validate='m:1'
) # 722

## 2.4b) case-insensitive enzyme synonym matches
enzyme_casei_syn, remf = join_out_lower(
    enzymedfi.select('context_id', 'enzyme_id', 'synonyms_lower').explode('synonyms_lower'),
    remf,
    'synonyms_lower',
    # validate='m:1'
) # 188

### 2.6: organisms
### 2.7: substrates
subs_casei_full, remf = join_out_lower(
    substratedfi.select('context_id', 'substrate_id', 'fullname_lower'),
    remf,
    'fullname_lower',
) # 1_195
subs_casei_syn, remf = join_out_lower(
    substratedfi.select('context_id', 'substrate_id', 'synonyms_lower').explode('synonyms_lower'),
    remf,
    'synonyms_lower',
    # validate='m:1'
) # 505

remf = remf.drop('fragment_lower', strict=False)
##### STEP 3.x: substrings

remf = remf.with_columns(
    pl.col('fragments').alias('shrinkable')
)

### 3.0: "wild-type"
wt_inexact, remf = subfilter_out(r_wt_inexact, remf) # 5_659

### 3.1: pH
pH_inexact, remf = subfilter_out(r'(pH( =)? \d+(\.\d+)?( ?- ?\d+(\.\d+)?)?)', remf)

### 3.2: temp
temp_inexact, remf = subfilter_out(r_lite(r_temp_range), remf)

### 3.4: mutants
# substring match is actually TERRIBLE for ctxl because Q265K will be considered a substring of Q265K/Y195S
mutants_inexact_re, remf = subfilter_out(
    # r_mutant_many_2to4_amino1_legacy,
    r_mutant_omni,
    remf,
) # 10_145 -> 10_657

recombinant_inexact, _ = filter_out(r_recombinant, remf) # 6_742

### 3.5: enzymes

enzymedf_names = pl.concat([
        enzymedf.select('context_id', 'enzyme_id', 'fullname').rename({
            'fullname': 'enzyme_name',
        }).with_columns(
            pl.lit(True).alias('is_fullname')
        ),
        enzymedf_synonyms.rename({
            'synonyms': 'enzyme_name',
        }).with_columns(
            pl.lit(False).alias('is_fullname')
        )
    ], how='diagonal'
).sort(
    pl.col('enzyme_name').str.len_chars().over('context_id'),
    descending=True
)

enzymes_inexact, remf = subjoin_out(
    enzymedf_names,
    remf,
    'enzyme_name',
    drop_shrinkable=False,
) # 50_115
pass

### 3.6: organisms
# I place organisms after enzymes, because enzymes may contain an organism name
uniprotdf_names = load_uniprot_names()
organisms_inexact, remf = filter_list_out(
    uniprotdf_names['organism'].to_list(),
    remf,
    ascii_case_insensitive=True
) # 9_802 -> 13_852 -> 15_203

## 3.6d) exact organism matches (short genus), but no associated genus name
organisms_exact_sg_re, remf = filter_out(r_organisms_sg, remf, on='shrinkable', extract=True) # 979

### 3.7: cofactors

cofactors_inexact, remf = subfilter_out(r_cofactor_many, remf) # 8_335 -> 9_579 -> 11_136
cofactors_inexact_ends, remf = filter_out(r' [mnµ]M$', remf, on='shrinkable', extract=True) # 3_409
cofactors_ions, remf = filter_out(r_ions, remf, on='shrinkable', extract=False) # r'[A-Z][a-z][²³]?⁺' # 3_210

## 3.8: organs

organs, remf = subfilter_out(r'(?i)\b(heart|lung|brain|kidney|liver|stomach|(small |large )?intestine|colon|pancreas|gallbladder|ovary|ovarie|test[ie]s|uterus|skin|bladder|spleen|thyroid( gland)?)s?\b', remf) # 1_195 # colon

# purified enzyme; soluble; 
# free enzyme; control; 
# expressed in

# \d+ μg/ml
# \d+ [mn]M
# \d0%
# ΔP =
# MPa

# common ion cofactors:
# NADP+

# phase, case, base, release, disease, increase, decrease

# Mg2+ Ca2+ Na+ K+ Cl-
# 220_955
# 189_572 -> 184_089 -> 180_822 -> 178_256
pass