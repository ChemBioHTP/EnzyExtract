"""
see also:
enzyextract.hungarian.csv_fix.pl_widen_df
"""


import polars as pl

# from enzyextract.post.regexes import r_hyphens, r_mutant_2to4, r_pH_range, r_recombinant, r_temp_kelvin, r_temp_range, r_unclassified, r_wt_exact, r_wt_inexact, unicode_fix
from enzyextract.post.regexes import *

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
contextdf = contextdf.with_columns(
    unicode_fix_list('temperatures'),
    unicode_fix_list('pHs'),
)

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

def filter_out(regex: str, remf: pl.DataFrame) -> pl.DataFrame:
    """
    Filter a DataFrame by a regex pattern.
    """
    mask = remf.select(
        pl.col('fragments').str.contains(regex)
    ).fill_null(False).to_series()
    subset = remf.filter(mask)
    remf = remf.filter(~mask)
    return subset, remf


def subfilter_out(regex: str, remf: pl.DataFrame) -> pl.DataFrame:
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
    ).drop_nulls('extract').drop('shrinkable').drop('fragment_lower', strict=False)
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

def subjoin_out(rhs: pl.DataFrame, remf: pl.DataFrame, right_on: str, **kwargs) -> pl.DataFrame:
    # 1. cartesian-esque join
    product = remf.join(
        rhs,
        left_on=['context_id'],
        right_on=['context_id'],
        how='inner',
        **kwargs
    )
    subset = product.filter(
        pl.col('shrinkable').str.contains(pl.col(right_on), literal=True)
    ).rename({
        right_on: 'extract'
    }) # .drop(right_on)
    successful_matches = subset.select(
        'fragment_id', 
        'extract'
    ).unique('fragment_id', keep='first')
    successful_remf = remf.join(successful_matches, on='fragment_id', validate='m:1', how='inner', coalesce=True)
    rest_remf = remf.join(successful_matches, on='fragment_id', how='anti')

    successful_remf = successful_remf.with_columns(
        pl.when(pl.col('extract').is_not_null()).then(
            pl.col('shrinkable')
            # .str.replace(pl.col('extract'), '', literal=True)
            # https://github.com/pola-rs/polars/issues/14367
            .str.replace(pl.col('extract').first(), '', literal=True).over('extract')
            .str.replace(r', (, |$)', '') # remove extra commas
            .str.strip_chars()
            .alias('shrinkable')
        ).otherwise(pl.col('shrinkable'))
    ).filter(
        pl.col('shrinkable').str.len_chars() > 0 # remove empty strings
    )
    successful_remf.drop_in_place('extract')
    remf = rest_remf.merge_sorted(successful_remf, key='fragment_id')
    subset = subset.drop('shrinkable').drop('fragment_lower', strict=False)
    return subset, remf

### STEP 1: pH
### STEP 2: temperature

## 1a. exact pH matches
pH_exact, remf = filter_out(r_pH_range, remf)
# print(pH_exact) # 28_231

## 2a. exact temperature matches

temp_exact, remf = filter_out(r_temp_range, remf)
# kelvin is delayed to after step 4 (enzymes)
# print(temp_exact) # 23_372 -> 23_761




### STEP 3: cofactors (tags that begin with 'with')

cofactors, remf = filter_out(r'^\(?[Ww]ith ', remf)
# print(cofactors) # 17_798

### STEP 4: enzyme names
enzymedf = contextdf.select('context_id', 'enzymes').explode('enzymes').unnest('enzymes').sort('context_id').with_row_index('enzyme_id')
# enzymedf = enzymedf.with_columns(
#     unicode_fix(pl.col('fullname')),
#     unicode_fix_list('synonyms'),
#     unicode_fix_list('organisms'),
#     unicode_fix_list('mutants'),
# )
enzymedf_synonyms = enzymedf.select('context_id', 'enzyme_id', 'synonyms').explode('synonyms')


## 4a. exact fullname matches
enzyme_exact_full, remf = join_out(
    enzymedf.select('context_id', 'enzyme_id', 'fullname'),
    remf,
    'fullname',
    # validate='m:1'
)
# print(enzyme_exact_full) # 43_906

## 4b. exact synonym matches
enzyme_exact_syn, remf = join_out(
    enzymedf_synonyms,
    remf,
    'synonyms',
    # validate='m:1'
)
# print(enzyme_exact_syn) # 90_470

### STEP 5: mutants
## 5a. "wild-type"
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


### STEP 6: organisms
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


organisms_exact, remf = join_out(
    organismdf,
    remf,
    'organisms',
) # 16_544

organisms_exact_sg, remf = join_out(
    organisms_sg,
    remf,
    'organism_sg',
) # 2_913

# these matches kind of suck: they have some false positives and/or also remove some 
# additional information
# hence, do not remove them from the remaining fragments

## 5c. inexact mention of "mutant" or "recombinant"
# removing from fragments leaves the risk of removing enzyme or organism information
# remf = remf.filter(~pl.col('fragment_id').is_in(set(recombinant_inexact['fragment_id'])))

## 5d. regex for point mutants
# single-digit mutants are risking, because it would loop in false positives like "H2O" or "D2O"
# 2+ digits are safer
mutants_exact_re, remf = filter_out(
    # rf"^([Mm]utant )?{r_mutant_many_2to4_amino1_legacy}( [Mm]utant)?$",
    r'^' + r_mutant_omni + r'$',
    remf
) # 3_339 -> 3_769

recombinant_inexact, _ = filter_out(r_recombinant, remf) # 6_742
# 4_242

# 242_348 (half) remain

### STEP 7: extra substrates
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

### STEP 8: kelvin (needs to be done after enzymes)
temp_exact_k, remf = filter_out(r_temp_kelvin, remf)



# filter out known unclassified fragments
unclassified_known, remf = filter_out(r_unclassified, remf)

##### STEP 1.x: case insensitive


remf = remf.with_columns(
    pl.col('fragments').str.to_lowercase().alias('fragment_lower'),
)
enzymedfi = enzymedf.with_columns(
    pl.col('fullname').str.to_lowercase().alias('fullname_lower'),
    pl.col('synonyms').list.eval(pl.element().str.to_lowercase()).alias('synonyms_lower'),
)

### 1.4: enzymes

## 1.4a) case-insensitive enzyme fullname matches
enzyme_casei_full, remf = join_out_lower(
    enzymedfi.select('context_id', 'enzyme_id', 'fullname_lower'),
    remf,
    'fullname_lower',
    # validate='m:1'
) # 722

## 1.4b) case-insensitive enzyme synonym matches
enzyme_casei_syn, remf = join_out_lower(
    enzymedfi.select('context_id', 'enzyme_id', 'synonyms_lower').explode('synonyms_lower'),
    remf,
    'synonyms_lower',
    # validate='m:1'
) # 188

### 1.6: organisms
### 1.7: substrates


# letterless = remf.filter(
#     pl.col('fragments').str.contains(r'^[^\w]*$')
# ) # 34, mostly (+) or (-)
# pass

##### STEP 2.x: substrings

remf = remf.with_columns(
    pl.col('fragments').alias('shrinkable')
)


pH_inexact, remf = subfilter_out(r'(pH( =)? \d+(\.\d+)?( ?- ?\d+(\.\d+)?)?)', remf)
temp_inexact, remf = subfilter_out(r_temp_range_lite, remf)
wt_inexact, remf = subfilter_out(r_wt_inexact, remf) # 5_659
cofactors_inexact, remf = subfilter_out(r'^\d+(\.\d+)? [mnµ]M [^,]*', remf)
cofactors_ions, remf = filter_out(r'[A-Z][a-z][²³]?⁺', remf)
# print(enzyme_exact_syn) # 90_470

enzymes_inexact_full, remf = subjoin_out(
    enzymedf.select('context_id', 'enzyme_id', 'fullname'),
    remf,
    'fullname',
) # 1_200

# substring match is actually TERRIBLE for ctxl because Q265K will be considered a substring of Q265K/Y195S
mutants_inexact_re, remf = subfilter_out(
    # r_mutant_many_2to4_amino1_legacy,
    r_mutant_omni,
    remf,
) # 10_145 -> 10_548

enzymes_inexact_syn, remf = subjoin_out(
    enzymedf_synonyms,
    remf,
    'synonyms',
) # 1_200

##

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

# Mg2+ Ca2+ Na+ K+ Cl-
# 220_955
pass