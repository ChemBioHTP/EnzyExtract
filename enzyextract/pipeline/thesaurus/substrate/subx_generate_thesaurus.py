import polars as pl
import polars.selectors as cs

# note: brenda/pubchem idents never have ascii
replace_greek = {'α': 'alpha', 'β': 'beta', 'ß': 'beta',
                 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'omicron', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega'}
replace_nonascii = {'\u2018': "'", '\u2019': "'", '\u2032': "'", '\u201c': '"', '\u201d': '"', '\u2033': '"',
                    '\u00b2': '2', '\u00b3': '3',
                    '\u207a': '+', # '\u207b': '-', # '\u207c': '=', '\u207d': '(', '\u207e': ')',
                    '(±)-': '', # we cannot handle the ± character anyways
                    '®': '',
                    }

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