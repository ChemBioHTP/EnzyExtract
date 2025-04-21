import os
import re

from Bio.Data.IUPACData import protein_letters_3to1_extended
from sklearn.metrics import mean_absolute_error, mean_squared_error

from enzyextract.thesaurus.mutant_patterns import amino3
from datetime import datetime
import polars as pl
import polars.selectors as cs
import rapidfuzz

from enzyextract.hungarian.hungarian_matching import is_wildtype
from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit
from enzyextract.hungarian import pl_hungarian_match
from enzyextract.thesaurus.mutant_patterns import mutant_pattern, mutant_v3_pattern

# note: brenda/pubchem idents never have ascii
replace_greek = {'α': 'alpha', 'β': 'beta', 'ß': 'beta',
                 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'omicron', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega'}
replace_nonascii = {'\u2018': "'", '\u2019': "'", '\u2032': "'", '\u201c': '"', '\u201d': '"', '\u2033': '"',
                    '\u00b2': '2', '\u00b3': '3',
                    '\u207a': '+', # '\u207b': '-', # '\u207c': '=', '\u207d': '(', '\u207e': ')',
                    '(±)-': '', # we cannot handle the ± character anyways
                    '®': '',
                    }
replace_amino_acids = protein_letters_3to1_extended



def script_substrate_idents(base_df, abbr_df, gpt_df):
    """
    load up substrate idents. form a thesaurus
    """
    print("Regenerating substrate thesaurus (will cache)")
    


    ### add substrate_unabbreviated to base_df
    
    abbr_df = abbr_df.select(['pmid', 'abbreviation', 'full_name']).rename({'full_name': 'substrate_unabbreviated'})
    base_df = base_df.join(abbr_df, left_on=['pmid', 'substrate'], right_on=['pmid', 'abbreviation'], how='left')
    base_df = base_df.with_columns([
        pl.coalesce(['substrate_full', 'substrate_unabbreviated']).alias('substrate_full')
    ]).select(cs.exclude('substrate_unabbreviated'))
    del abbr_df

    want = base_df['substrate'].unique(maintain_order=True)
    want = pl.concat([want, base_df['substrate_full'].unique(maintain_order=True)])

    # those_pmids = set(base_df['pmid'].unique())
    # del base_df

    
    # gpt_df = gpt_df.filter(pl.col('pmid').is_in(those_pmids))
    want = pl.concat([want, gpt_df['substrate'].unique(maintain_order=True)])
    want = pl.concat([want, gpt_df['substrate_full'].unique(maintain_order=True)])
    those_pmids = set(gpt_df['pmid'].unique()) # NVM: beluga's set is actually what we want to filter pmids to

    if 'substrate_2' in gpt_df.columns:
        # this is a file matched with brenda
        want = pl.concat([want, gpt_df['substrate_2'].unique(maintain_order=True)])
    del gpt_df

    brenda_kinetic = pl.read_parquet('data/brenda/brenda_kcat_v3slim.parquet')
    brenda_kinetic = brenda_kinetic.filter(pl.col('pmid').is_in(those_pmids))
    want = pl.concat([want, brenda_kinetic['substrate'].unique(maintain_order=True)])
    del brenda_kinetic

    # note: alpha and beta are never used in brenda/pubchem substrate names
    # (i think they do not use unicode)
    want = (
        want
        .unique(maintain_order=True).drop_nulls()
    )
    want_df = want.to_frame('name').with_columns([
        pl.col('name')
        .str.to_lowercase()
        .str.replace_all('[-᠆‑‒–—―﹘﹣－˗−‐⁻]', '-')
        .str.replace_many(replace_greek)
        .str.replace_many(replace_nonascii)
        .alias('name_lower'),
    ]).with_columns([
        (pl.col('name_lower').str.find('[^ -~]').is_not_null()).alias('not_ascii')
    ])
    want_names = set(want_df['name_lower'])

    # del base_df, gpt_df

    # add cids
    cids = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Synonym-filtered.parquet')
    cids = cids.filter(
        pl.col('name').str.to_lowercase().is_in(want_names)
    ).with_columns([
        pl.col('name').str.to_lowercase().alias('name_lower')
    ])

    cids_gb = cids.group_by('name_lower').agg(pl.col('cid')) # name_lower to cids
    want_df = want_df.join(cids_gb, left_on='name_lower', right_on='name_lower', how='left')
    # Add the headers as a new row

    # Rename the columns

    brendas = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')
    brendas = brendas.filter(
        pl.col('name').str.to_lowercase().is_in(want_names)
    ).with_columns([
        pl.col('name').str.to_lowercase().alias('name_lower')
    ])
    brenda_gb = brendas.group_by('name_lower').agg(pl.col('brenda_id').unique()) # name to inchi

    # gets 50% and 57% of the names
    # compare before: 49% and 56%
    want_df = want_df.join(brenda_gb, left_on='name_lower', right_on='name_lower', how='left')
    # print(want)
    return want_df
    

def load_ecs(add_top3=True):
    """
    return the ec df, with these columns:
    - alias
    - enzyme_ecs
    - enzyme_ecs_top3
    """
    ecs = pl.read_parquet('data/brenda/brenda_to_ec.parquet')
    selectors = ['alias', 'enzyme_ecs']
    if add_top3:
        selectors.append('enzyme_ecs_top3')
    ecs = (
        ecs
        .rename({'viable_ecs': 'enzyme_ecs'})
        .select(selectors)
    )

    return ecs
    

# def sequenced():
#     df = latest_smiles_df()
#     # print(df)


#     df2 = latest_inchi_df()

#     brenda_df = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')
#     print(df2)


### Begin methods for use for assignment problem
def is_subset_match(list1: list, list2: list):
    """
    Returns True if list1 is a subset of list2, or vice versa.
    Is symmetric
    """
    if not(list1) or not(list2):
        return False
    return set(list1) <= set(list2) or set(list2) <= set(list1)

def is_one_to_many_match(list1: list, list2: list):
    """
    Returns True if WLOG list1 has only one element, and it is in list2
    """
    if len(list1) == 1 and list1[0] in list2:
        return True
    if len(list2) == 1 and list2[0] in list1:
        return True
    return False

def organism_objective(gpt_dict, base_dict):
    if gpt_dict.get('organism') and base_dict.get('organism'):
        if gpt_dict['organism'].lower() == base_dict['organism'].lower():
            return 1
    return 0

BEST = 9
def enzyme_objective(gpt_dict, base_dict, do_top3=False):
    # Objective function. Tries to maximize the number of enzyme-substrate pairs that are the same
    gpt_names = [x for x in [gpt_dict['enzyme'], gpt_dict.get('enzyme_full')] if x]
    base_names = [x for x in [base_dict['enzyme'], base_dict.get('enzyme_full')] if x]
    if do_top3:
        gpt_ecs = [x for x in [gpt_dict['enzyme_ecs_top3'], gpt_dict.get('enzyme_ecs_top3_full')] if x]
        base_ecs = [x for x in [base_dict['enzyme_ecs_top3'], base_dict.get('enzyme_ecs_top3_full')] if x]
    else:
        gpt_ecs = [x for x in [gpt_dict['enzyme_ecs'], gpt_dict.get('enzyme_ecs_full')] if x]
        base_ecs = [x for x in [base_dict['enzyme_ecs'], base_dict.get('enzyme_ecs_full')] if x]
    # if one is completely empty, then it will always be 0


    ### One-to-one match: the best
    for gpt_name in gpt_names:
        for base_name in base_names:
            if gpt_name.lower() == base_name.lower():
                return BEST # case-insensitive match is good enough
    
    for gpt_ec in gpt_ecs:
        for base_ec in base_ecs:
            if gpt_ec == base_ec:
                return BEST
    
    ### One-to-many match: this is okay
    for gpt_ec in gpt_ecs:
        for base_ec in base_ecs:
            if is_subset_match(gpt_ec, base_ec):
                return BEST-1

    ### A high string similarity is also in this calibre
    for gpt_name in gpt_names:
        for base_name in base_names:
            if len(gpt_name) >= 5 and len(base_name) >= 5 and rapidfuzz.fuzz.ratio(gpt_name, base_name) > 90:
                return BEST-1
    
    ### Many-to-many match: this is fine
    for gpt_ec in gpt_ecs:
        for base_ec in base_ecs:
            if len(set(gpt_ec) & set(base_ec)) > 0:
                return BEST-3
    return 0
def substrate_objective(gpt_dict, base_dict):
    # Objective function. Tries to maximize the number of substrate pairs that are the same
    
    ### One-to-one match: the best
    gpt_names = [x for x in [gpt_dict['substrate'], gpt_dict.get('substrate_full')] if x]
    base_names = [x for x in [base_dict['substrate'], base_dict.get('substrate_full')] if x]
    gpt_cids = [x for x in [gpt_dict['cid'], gpt_dict.get('cid_full')] if x]
    base_cids = [x for x in [base_dict['cid'], base_dict.get('cid_full')] if x]
    gpt_brendas = [x for x in [gpt_dict['brenda_id'], gpt_dict.get('brenda_id_full')] if x]
    base_brendas = [x for x in [base_dict['brenda_id'], base_dict.get('brenda_id_full')] if x]
    
    ### One-to-one match: the best
    for gpt_name in gpt_names:
        for base_name in base_names:
            if gpt_name.lower() == base_name.lower():
                return BEST
    for gpt_cid in gpt_cids:
        for base_cid in base_cids:
            if gpt_cid == base_cid:
                return BEST
    for gpt_brenda in gpt_brendas:
        for base_brenda in base_brendas:
            if gpt_brenda == base_brenda:
                return BEST
    
    ### One-to-many match: this is okay
    for gpt_cid in gpt_cids:
        for base_cid in base_cids:
            if is_subset_match(gpt_cid, base_cid):
                return BEST-1
    for gpt_brenda in gpt_brendas:
        for base_brenda in base_brendas:
            if is_subset_match(gpt_brenda, base_brenda):
                return BEST-1
    

    ### Many-to-many match: this is fine
    for gpt_cid in gpt_cids:
        for base_cid in base_cids:
            if len(set(gpt_cid) & set(base_cid)) > 0:
                return BEST-2
    for gpt_brenda in gpt_brendas:
        for base_brenda in base_brendas:
            if len(set(gpt_brenda) & set(base_brenda)) > 0:
                return BEST-2
            
    ### A high string similarity is slightly worse - eg. NAD vs NAD+
    for gpt_name in gpt_names:
        for base_name in base_names:
            if len(gpt_name) >= 5 and len(base_name) >= 5 and rapidfuzz.fuzz.ratio(gpt_name, base_name) > 90:
                return BEST-3
    return 0

def mutant_objective(gpt_dict, base_dict):
    """
    Reward if the mutants are the same
    """

    a = gpt_dict['clean_mutant']
    b = base_dict['clean_mutant']
    if a and b:
        if set(a) == set(b):
            return BEST

    a = gpt_dict['mutant']
    b = base_dict['mutant']
    
    if a and b:
        # if both not empty
        if a.lower() == b.lower(): # .lower() == b.lower():
            return BEST
            # ensure it is a mutant format
            # if re.match(r"\b[A-Z]\d+[A-Z]\b", a): 
            #     return 9
        if is_wildtype(a, allow_empty=False) and is_wildtype(b, allow_empty=False):
            return BEST # both wild-type: good.
    
    # allow matching empty to "wild-type", but don't reward it as much
    # if (a or b) and is_wildtype(a) and is_wildtype(b):
        # return 0.2
    
    # if both not empty
    
    return 0
def pH_objective(gpt_dict, base_dict):
    """
    Reward if the pH values are the same
    """
    a = gpt_dict['pH']
    b = base_dict['pH']
    if a and b:
        if a == b:
            return 1
    return 0
def temperature_objective(gpt_dict, base_dict):
    """
    Reward if the temperature values are the same
    """
    a = gpt_dict['temperature']
    b = base_dict['temperature']
    if a and b:
        if a == b:
            return 1
    return 0
def kinetic_objective(gpt_dict, base_dict):
    a = gpt_dict['kcat_value'] 
    b = base_dict['kcat_value']
    if a and b and (abs(a - b) / min(a, b) < 0.1): # reward if within 10%
        return 1
    return 0
def enzyme_substrate_objective(gpt_dict, base_dict):
    """
    
    """
    organism_coeff = 10000
    enzyme_coeff = 1000
    mutant_coeff = 100
    
    substrate_coeff = 10
    ph_coeff = 0.1
    temperature_coeff = 0.1
    kinetic_coeff = 0.01
    z = (
        organism_coeff * organism_objective(gpt_dict, base_dict)
        + enzyme_coeff * enzyme_objective(gpt_dict, base_dict)
        + mutant_coeff * mutant_objective(gpt_dict, base_dict)
        + substrate_coeff * substrate_objective(gpt_dict, base_dict)
        + ph_coeff * pH_objective(gpt_dict, base_dict)
        + temperature_coeff * temperature_objective(gpt_dict, base_dict)
        + kinetic_coeff * kinetic_objective(gpt_dict, base_dict)
    )
    return z

def parse_col(col: str, suffix='') -> pl.Expr:
    """
    convert kcat (string) to float
    """
    def to_true(x):
        value, unit, _ = parse_value_and_unit(x + suffix)
        return convert_to_true_value(value, unit)
    return pl.col(col).map_elements(to_true, return_dtype=pl.Float64)


def _remove_bad_es_calc_kcat_value_and_clean_mutants(df: pl.DataFrame):
    """
    Perform various data cleaning with 

    1. remove substrate_full if it is the same as substrate
    2. remove enzyme_full if it is the same as enzyme
    3. convert km and kcat to float
    """
    if 'substrate_full' in df.columns and 'enzyme_full' in df.columns:
        df = df.with_columns([
            pl.when(pl.col('substrate_full') == pl.col('substrate'))
            .then(None)
            .otherwise(pl.col('substrate_full')).alias('substrate_full'),
            pl.when(pl.col('enzyme_full') == pl.col('enzyme'))
            .then(None)
            .otherwise(pl.col('enzyme_full')).alias('enzyme_full'),
        ])
    if 'km_value' not in df.columns:
        df = df.with_columns([
            parse_col("km").alias("km_value"),
        ])
    if 'kcat_value' not in df.columns:
        df = df.with_columns([
            parse_col("kcat").alias("kcat_value"),
        ])
    # if 'clean_mutant'
    standardize_mutants1_re = re.compile(rf"({amino3})-?(\d{{1,4}})(\s?→\s?| to |\s?>\s?|!)[ -]?({amino3})") # if arrow or "to", then it is unambiguously a point mutation.
    df = df.with_columns([
        pl.col("mutant").str.replace_all(standardize_mutants1_re.pattern, r"$1$2$4").alias("mutant")
    ])
    df = df.with_columns([
        pl.col("mutant").str.extract_all(mutant_pattern.pattern).alias("mutant1"),
        pl.col("mutant").str.extract_all(mutant_v3_pattern.pattern).alias("mutant3"),
    ])

    df = df.with_columns([
        pl.col("mutant3").list.eval(pl.element().str.replace_many(replace_amino_acids)).alias("mutant3"),
    ]).with_columns([
        pl.col("mutant1").list.concat(pl.col("mutant3")).alias("clean_mutant")
    ]).select(cs.exclude('mutant1', 'mutant3'))
    return df
def script_match_base_gpt(want_df: pl.DataFrame, base_df: pl.DataFrame, gpt_df: pl.DataFrame):


    # base_df = pl.read_csv('data/humaneval/rumble/rumble_20241205_ec.csv', schema_overrides=so)
    those_pmids = set(base_df['pmid'].unique())

    # gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)
    # inner join handles this
    gpt_df = gpt_df.filter(pl.col('pmid').is_in(those_pmids))

    base_df = base_df.select(cs.exclude('viable_ecs', 'enzyme_preferred')) # recalculate these

    # for debug, ease of visualization
    # base_df = base_df.select(['pmid', 'organism', 'mutant', 'enzyme', 'enzyme_full', 'substrate', 'substrate_full'])
    # gpt_df = gpt_df.select(['pmid', 'organism', 'mutant', 'enzyme', 'enzyme_full', 'substrate', 'substrate_full'])
    
    
    ### add substrate_unabbreviated to base_df
    abbr_df = pl.read_parquet('data/thesaurus/abbr/beluga-abbrs-4ostruct_20241213.parquet')
    abbr_df = abbr_df.select(['pmid', 'abbreviation', 'full_name']).rename({'full_name': 'substrate_unabbreviated'})
    base_df = base_df.join(abbr_df, left_on=['pmid', 'substrate'], right_on=['pmid', 'abbreviation'], how='left')
    base_df = base_df.with_columns([
        pl.coalesce(['substrate_full', 'substrate_unabbreviated']).alias('substrate_full')
    ])



    # if substrate_full is same as substrate, then we don't need it
    base_df = _remove_bad_es_calc_kcat_value_and_clean_mutants(base_df)
    gpt_df = _remove_bad_es_calc_kcat_value_and_clean_mutants(gpt_df)


    ### add cid and brenda_id to base_df
    want_view = want_df.select(['name', 'cid', 'brenda_id'])
    base_df = base_df.join(want_view, left_on='substrate', right_on='name', how='left')
    base_df = base_df.join(want_view, left_on='substrate_full', right_on='name', how='left', suffix='_full')

    ### add cid and brenda_id to gpt_df
    if 'cid' not in gpt_df.columns or 'brenda_id' not in gpt_df.columns:
        gpt_df = gpt_df.join(want_view, left_on='substrate', right_on='name', how='left')
        gpt_df = gpt_df.join(want_view, left_on='substrate_full', right_on='name', how='left', suffix='_full')

    ### add ecs to base_df
    ecs = load_ecs()
    base_df = base_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
    base_df = base_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')

    ### add ecs to gpt_df
    if 'enzyme_ecs' not in gpt_df.columns:
        gpt_df = gpt_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
        gpt_df = gpt_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')
    # now, give 

    matched_df = pl_hungarian_match.join_optimally(
        gpt_df, 
        base_df, 
        enzyme_substrate_objective,
        partition_by='pmid',
        how='inner',
        progress_bar=True,
        maximize=True,
        objective_column='objective'
    )
    return finalize_df(matched_df)

    
def finalize_df(matched_df):

    matched_view = matched_df.with_columns([
        ((pl.col('objective').cast(pl.Int16) % 10000) >= 8000).alias('same_enzyme'),
        ((pl.col('objective').cast(pl.Int16) % 1000) >= 800).alias('same_mutant'),
        ((pl.col('objective').cast(pl.Int16) % 100) >= 80).alias('same_substrate'),
        # parse_col('km_1').alias('km_value_1'),
        # parse_col('km_2').alias('km_value_2'),
        # parse_col('kcat_1').alias('kcat_value_1'),
        # parse_col('kcat_2').alias('kcat_value_2'),
    ]).with_columns([
        ((pl.col('clean_mutant_1').list.len() > 0) 
         & (pl.col('clean_mutant_2').list.len() > 0) 
         & ~pl.col('same_mutant')).alias('different_mutant'),
    ]).with_columns([
        # same_mutant is True -> different_mutant is False

        # same_mutant is True -> True
        # same_mutant is False and different_mutant if False -> None
        # same_mutant is False and different_mutant is False -> False
        # different_mutant is None -> None
        pl.when(
            (~pl.col("same_mutant") & ~pl.col("different_mutant"))
            | pl.col("different_mutant").is_null()
        ).then(None).otherwise(
            pl.col("same_mutant")
        ).alias("same_mutant"),
    ])

    

    matched_view = add_diff(matched_view)
    # 
        
    selectors = [
        'pmid', 'organism_1', 'organism_2', 
        'mutant_1', 'clean_mutant_1',
        'mutant_2', 'clean_mutant_2',
        'enzyme_1', 'enzyme_2', 'enzyme_full_1', 'enzyme_full_2',
        'substrate_1', 'substrate_2', 'substrate_full_1', 'substrate_full_2', 
        'km_1', 'km_2', 'km_value_1', 'km_value_2', 
        'kcat_1', 'kcat_2', 'kcat_value_1', 'kcat_value_2',
        'src_2', 'objective',
        'enzyme_ecs_1', 'enzyme_ecs_2',
        'cid_1', 'cid_2', 'brenda_id_1', 'brenda_id_2',
        # 'different_mutant', 
        'same_mutant', # 'same_mutant_dev', 
        'same_enzyme', 'same_substrate',
        'km_diff', 'kcat_diff',
    ]
    selectors = [s for s in selectors if s in matched_view.columns]
    matched_view = matched_view.select(selectors)

    # to recover the original objective:
    # substrate: need [0-1]. remove as much 5 as possible from objective
    # enzyme: 




        # Create expression to parse both columns
    return matched_view


    # matched_view.write_parquet('_debug/cache/beluga_matched_based_on_EnzymeSubstrate.parquet')



def script_match_brenda_gpt(want_df: pl.DataFrame, gpt_df: pl.DataFrame):
    """
    match gpt aagainst brenda
    """

    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
    # _base_df = pl.read_csv('data/humaneval/rumble/rumble_20241205_ec.csv', schema_overrides=so)
    # del _base_df

    # gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)
    those_pmids = set(gpt_df['pmid'].unique())

    # brenda_df = pl.read_parquet('data/brenda/brenda_kcat_v3slim.parquet')
    brenda_df = pl.read_parquet('data/brenda/brenda_kcat_cleanest.parquet')
    brenda_df = brenda_df.filter(pl.col('pmid').is_in(those_pmids))
    brenda_df = brenda_df.rename({'organism_name': 'organism', 'turnover_number': 'kcat', 'km_value': 'km'})

    # if brenda reports kcat or km as a range ( -- ), then we don't need it
    pmids_with_brenda_range = brenda_df.filter(
        pl.col('kcat').str.contains(' -- ')
        | pl.col('km').str.contains(' -- ')
    ).select('pmid').unique()
    
    brenda_df = brenda_df.filter(~pl.col('pmid').is_in(pmids_with_brenda_range))
    brenda_df = brenda_df.with_columns([
        # brenda can direct convert
        (pl.col("km").cast(pl.Float64, strict=False) / 1000).alias("km_value"),
        pl.col("kcat").cast(pl.Float64, strict=False).alias("kcat_value"),
    ])

    # if substrate_full is same as substrate, then we don't need it
    # brenda_df is OK
    gpt_df = _remove_bad_es_calc_kcat_value_and_clean_mutants(gpt_df)
    brenda_df = _remove_bad_es_calc_kcat_value_and_clean_mutants(brenda_df)

    ### add cid and brenda_id to base_df
    want_view = want_df.select(['name', 'cid', 'brenda_id'])
    brenda_df = brenda_df.join(want_view, left_on='substrate', right_on='name', how='left')

    ### add cid and brenda_id to gpt_df
    if 'cid' not in gpt_df.columns or 'brenda_id' not in gpt_df.columns:
        gpt_df = gpt_df.join(want_view, left_on='substrate', right_on='name', how='left')
        gpt_df = gpt_df.join(want_view, left_on='substrate_full', right_on='name', how='left', suffix='_full')

    ### add ecs to base_df
    ecs = load_ecs()
    brenda_df = brenda_df.join(ecs, left_on='enzyme', right_on='alias', how='left')

    ### add ecs to gpt_df
    if 'enzyme' in gpt_df.columns and 'enzyme_ecs' not in gpt_df.columns:
        gpt_df = gpt_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
        gpt_df = gpt_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')
    # now, give 
    

    matched_df = pl_hungarian_match.join_optimally(
        gpt_df, 
        brenda_df, 
        enzyme_substrate_objective,
        partition_by='pmid',
        how='inner',
        progress_bar=True,
        objective_column='objective'
    )

    return finalize_df(matched_df)

def add_diff(df):
    return df.with_columns(
        (
            pl.when(
                (pl.col('km_value_1') > 0)
                & (pl.col('km_value_2') > 0)
            ).then(
                (10 ** (pl.col('km_value_1').log10() - pl.col('km_value_2').log10()).abs())
            ).otherwise(
                None
            ).alias('km_diff')
        ),
        (
            pl.when(
                (pl.col('kcat_value_1') > 0)
                & (pl.col('kcat_value_2') > 0)
            ).then(
                (10 ** (pl.col('kcat_value_1').log10() - pl.col('kcat_value_2').log10()).abs())
            ).otherwise(
                None
            ).alias('kcat_diff')
        )
    )

def load_rumble_df(exclude_train=False):
    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
    base_df = pl.read_csv('data/humaneval/rumble/rumble_20241219.csv', schema_overrides=so)

    if exclude_train:
        train_pmids = pl.read_parquet('data/pmids/t2neboth_train.parquet')
        base_df = base_df.filter(~pl.col('pmid').is_in(train_pmids['pmid']))
    return base_df


def main(
    working: str,
    against_known: str,
    scino_only: str,
    whitelist: str,
    gpt_df: pl.DataFrame, # the unknown df
    known_df: pl.DataFrame, # the known df
    is_brenda: bool = False,
):
    so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}

    # step 1: thesaurus
    if not os.path.exists((want_dest := 'data/thesaurus/substrate/apogee_substrate_thesaurus.parquet')): #  or True:
        
        # base_df = pl.read_csv('data/humaneval/rumble/rumble_20241205_ec.csv', schema_overrides=so)
        known_df = load_rumble_df()
        abbr_df = pl.read_parquet('data/thesaurus/abbr/beluga-abbrs-4ostruct_20241213.parquet')
        # gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)
        # gpt_df = pl.read_parquet('data/_compiled/apogee_all.parquet')
        gpt_df = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')

        want_df = script_substrate_idents(gpt_df, abbr_df, gpt_df) # thesaurus
        want_df.write_parquet(want_dest)
        return
    else: want_df = pl.read_parquet(want_dest)


    

    # exclude scientific notation: exclude "10^" to see if it improves acc like I think
    
    if scino_only is True:
        gpt_df = gpt_df.filter(
            pl.col('kcat').str.contains('10\^')
            | pl.col('km').str.contains('10\^')
        )
        working += '_scientific_notation'
    elif scino_only is False:
        gpt_df = gpt_df.filter(
            (
                ~pl.col('kcat').str.contains('10\^')
                | pl.col('kcat').is_null()
            ) & (
                ~pl.col('km').str.contains('10\^')
                | pl.col('km').is_null()
            )
        )
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':

        bad_pmids = pl.read_parquet('data/revision/apogee-revision.parquet').filter(
            pl.col('kcat_scientific_notation')
        )
        gpt_df = gpt_df.filter(~pl.col('pmid').is_in(bad_pmids['pmid']))
        gpt_df = gpt_df.filter(
            ~pl.col('kcat').str.contains('10\^')
            & ~pl.col('km').str.contains('10\^')
        )
        working += '_no_scientific_revised'

    
    if whitelist is not None:
        if whitelist == 'hallucinated_micro':
            pmids_df = pl.read_parquet('data/pmids/apogee_hallucinated_micro.parquet')
        elif whitelist == 'wide_tables_only':
            pmids_df = pl.read_parquet('data/pmids/apogee_wide_tables_7plus.parquet')
        else:
            raise ValueError("Invalid whitelist")
        pmids = set(pmids_df['pmid'].unique())
        known_df = known_df.filter(pl.col('pmid').is_in(pmids))
        gpt_df = gpt_df.filter(pl.col('pmid').is_in(pmids))
        working += '_' + whitelist

    if is_brenda:
        matched_view = script_match_brenda_gpt(want_df, gpt_df) # matching
    else:
        matched_view = script_match_base_gpt(want_df, known_df, gpt_df)

    return matched_view
    # step 2b: match with brenda
    # _no_scientific_notation
    # if not os.path.exists(match_dest):
    # gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)

def gpt_locations():
    """Return the location of the gpt_df"""
    return {
        'beluga': 'data/valid/_valid_beluga-t2neboth_1.csv',
        'bucket': 'data/valid/_valid_bucket-rebuilt.parquet',
        'apogee': 'data/valid/_valid_apogee-rebuilt.parquet',
        'apatch': 'data/valid/_valid_apatch-rebuilt.parquet',
        'sabiork': 'data/sabiork/valid_sabiork.parquet',
        'everything': 'data/valid/_valid_everything.parquet',
        'thedata': 'data/export/TheData_kcat.parquet',
    }

def gpt_dataframe(working: str):
    """
    Return the gpt_df for the given working
    """
    fpath = gpt_locations()[working]
    if fpath.endswith('.csv'):
        gpt_df = pl.read_csv(fpath) # , schema_overrides=so)
    elif fpath.endswith('.parquet'):
        gpt_df = pl.read_parquet(fpath)
    else:
        raise ValueError("Unrecognized file type", fpath)
    return gpt_df

if __name__ == '__main__':
    raise NotImplementedError("This script is only an example.")
    
    # exit(0)
    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    # working = 'sabiork'
    # working = 'bucket'
    # working = 'apatch'
    # working = 'everything'
    working = 'thedata'

    # against = 'rumble'
    against = 'brenda'
    # against = 'sabiork'

    # scino_only = None
    # scino_only = True
    scino_only = False
    # scino_only = 'false_revised'

    whitelist = None
    # whitelist = 'wide_tables_only'
    # whitelist = 'hallucinated_micro'

    # step 2: matching
    # '_debug/cache/beluga_matched_based_on_EnzymeSubstrate.parquet'
    gpt_df = gpt_dataframe(working)

    if scino_only is True:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':
        working += '_no_scientific_revised'
    
    is_brenda = False
    if against == 'rumble':
        known_df = load_rumble_df(exclude_train=True)
    elif against == 'sabiork':
        known_df = pl.read_parquet('data/sabiork/valid_sabiork.parquet')
    else:
        known_df = None
        is_brenda = True


    matched_view = main(
        working=working,
        against_known=against,
        scino_only=scino_only,
        whitelist=whitelist,
        gpt_df=gpt_df,
        known_df=known_df,
        is_brenda=is_brenda,
    )
    fdir = f'data/metrics/{against}'
    os.makedirs(fdir, exist_ok=True)
    matched_view.write_parquet(f'{fdir}/{against}_{working}.parquet')
    pass