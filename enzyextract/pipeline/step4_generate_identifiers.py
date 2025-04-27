# join everything with identifiers

import polars as pl
import polars.selectors as cs
import re

from enzyextract.hungarian.hungarian_matching import is_wildtype
from enzyextract.hungarian.hungarian_matching import convert_to_true_value, parse_value_and_unit
from enzyextract.hungarian import pl_hungarian_match
from enzyextract.thesaurus.mutant_patterns import mutant_pattern, mutant_v3_pattern, amino3, amino3to1, standardize_mutants1_re, with_clean_mutants
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.organism_patterns import pl_fix_organism

# note: brenda/pubchem idents never have ascii
replace_greek = {'α': 'alpha', 'β': 'beta', 'ß': 'beta',
                 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'omicron', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega'}
replace_nonascii = {'\u2018': "'", '\u2019': "'", '\u2032': "'", '\u201c': '"', '\u201d': '"', '\u2033': '"',
                    '\u00b2': '2', '\u00b3': '3',
                    '\u207a': '+', # '\u207b': '-', # '\u207c': '=', '\u207d': '(', '\u207e': ')',
                    '(±)-': '', # we cannot handle the ± character anyways
                    '®': '',
                    }
def script_generate_thesaurus(gpt_df, base_df, abbr_df):
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

    # some more
    more_cids = []
    for filename in ['cid2title.tsv', 'cid2synonym.tsv']:
        more_df = pl.read_csv(f'data/substrates/cid/{filename}', separator='\t', quote_char=None)
        more_df = more_df.rename({'CID': 'cid', 'Name': 'name'})
        more_df = more_df.with_columns([
            pl.col('name').str.to_lowercase().alias('name_lower')
        ])
        more_cids.append(more_df)
    cids = pl.concat(more_cids + [cids])

    # cid to smiles
    cid2smiles = pl.read_csv('data/substrates/cid/cid2smiles.tsv', separator='\t', quote_char=None)
    cid2smiles = cid2smiles.rename({'CID': 'cid', 'SMILES': 'smiles'})
    cids = cids.join(cid2smiles, left_on='cid', right_on='cid', how='left')


    cids_gb = cids.group_by('name_lower').agg(
        pl.col('cid').unique(),
        pl.col('smiles').unique()) # name_lower to cids
    want_df = want_df.join(cids_gb, left_on='name_lower', right_on='name_lower', how='left')

    


    # Rename the columns

    ### add brenda_id to want_df
    brendas = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')
    brendas = brendas.filter(
        pl.col('name').str.to_lowercase().is_in(want_names)
    ).with_columns([
        pl.col('name').str.to_lowercase().alias('name_lower')
    ])

    #### but first, brenda to smiles
    brenda2smiles = pl.read_parquet('data/substrates/brenda/brenda_inchi_smiles.parquet').select('brenda_id', 'smiles')
    brenda2smiles = brenda2smiles.rename({'smiles': 'smiles_brenda'})
    brendas = brendas.join(brenda2smiles, left_on='brenda_id', right_on='brenda_id', how='left')
    
    brenda_gb = brendas.group_by('name_lower').agg(
        pl.col('brenda_id').unique(),
        pl.col('smiles_brenda').unique().drop_nulls()
    ).with_columns(
        pl.when(pl.col('smiles_brenda').list.len() == 0)
        .then(None).otherwise(pl.col('smiles_brenda')).alias('smiles_brenda'),
    ) # name_lower to brenda_ids and smiles

    # gets 50% and 57% of the names
    # compare before: 49% and 56%
    want_df = want_df.join(brenda_gb, left_on='name_lower', right_on='name_lower', how='left')
    # print(want)
    return want_df

def parse_col(col: str, suffix='', accept_unknown_unit=True) -> pl.Expr:
    """
    convert kcat (string) to float
    """
    def to_true(x):
        value, unit, _ = parse_value_and_unit(x + suffix)
        if unit is None and not accept_unknown_unit:
            return None
        return convert_to_true_value(value, unit)
    return pl.col(col).map_elements(to_true, return_dtype=pl.Float64)

def _remove_bad_es(df: pl.DataFrame):
    """
    Perform various data cleaning with 
    NOTE: see by_EnzymeSubstrate.py for the original code

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
    # if 'km_value' not in df.columns:
    # if 'kcat_value' not in df.columns:
    return df

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

def add_identifiers(gpt_df, subs_df):
    """
    Cleans up the gpt by standardizing mutants

    Add: EC, CID, BRENDA_ID
    """
    
    # NOTE: this was generated by by_EnzymeSubstrate.py
    # want_df = pl.read_parquet('data/thesaurus/substrate/apogee_substrate_thesaurus.parquet')
    want_df = subs_df

    # if substrate_full is same as substrate, then we don't need it
    gpt_df = _remove_bad_es(gpt_df)
    gpt_df = with_clean_mutants(gpt_df)

    ### add cid and brenda_id to gpt_df
    want_view = want_df.select(['name', 'cid', 'brenda_id', 'smiles', 'smiles_brenda'])
    gpt_df = gpt_df.join(want_view, left_on='substrate', right_on='name', how='left')
    gpt_df = gpt_df.join(want_view, left_on='substrate_full', right_on='name', how='left', suffix='_full')

    # simplify the smiles column, preferring substrate_full if it exists
    # then since we assume that multiple SMILES = synonyms, simply take the first one
    gpt_df = gpt_df.with_columns([
        pl.coalesce(['smiles_full', 'smiles', 'smiles_brenda_full', 'smiles_brenda']).list.get(0, null_on_oob=True).alias('smiles')
    ]).drop('smiles_full', 'smiles_brenda_full', 'smiles_brenda')

    ### add ecs to gpt_df
    ecs = load_ecs(add_top3=False)
    gpt_df = gpt_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
    gpt_df = gpt_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')

    # also add the true value
    gpt_df = gpt_df.with_columns(
        parse_col('kcat', accept_unknown_unit=False).alias('kcat_value'),
        parse_col('km', accept_unknown_unit=False).alias('km_value')
    )
    return gpt_df

def add_enzyme_sequences(gpt_df):
    ##### Add enzyme sequences

    gpt_df = gpt_df.with_columns([
        pl.lit(None).alias('sequence'),
        pl.lit(None).alias('sequence_source'),
        pl.lit(None).alias('uniprot'),
        pl.lit(None).alias('ncbi'),
        pl.lit(None).alias('pdb'),
        pl.lit(None).alias('max_enzyme_similarity'),
        pl.lit(None).alias('max_organism_similarity'),
        pl.lit(None).alias('total_similarity'),
    ])

    uniprot_conf = (
        pl.read_parquet('data/thesaurus/confident/uniprot.parquet')
        .with_columns(
            # multiplication of percentages
            # no organism present: confidence is 50%
            ((pl.col('max_enzyme_similarity') * pl.col('max_organism_similarity').fill_null(50)) / 100).alias('total_similarity'),
        )
    )
    pdb_conf = (
        pl.read_parquet('data/thesaurus/confident/pdb.parquet')
        .with_columns(
            # multiplication of percentages
            # no organism present: confidence is 50%
            ((pl.col('max_enzyme_similarity') * pl.col('max_organism_similarity').fill_null(50)) / 100).alias('total_similarity'),
        )
    )
    ncbi_conf = (
        pl.read_parquet('data/thesaurus/confident/ncbi.parquet')
        .with_columns(
            # multiplication of percentages
            # no organism present: confidence is 50%
            ((pl.col('max_enzyme_similarity') * pl.col('max_organism_similarity').fill_null(50)) / 100).alias('total_similarity'),
        )
    )
    ##### UNIPROT
    uniprot2seq = (
        pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
        .select(['uniprot', 'sequence']).filter(
            pl.col('sequence').is_not_null()
        ).unique('uniprot')
    )

    ### add uniprot (picked, pick-uniprot-prod2)
    uniprot_picked = (
        pl.read_parquet('data/thesaurus/enzymes/uniprot_picked.parquet')
        .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'uniprot'])
        .filter(pl.col('uniprot').is_not_null()) 
        .join(uniprot2seq, on='uniprot', how='left', validate='m:1') # adds sequence
        .rename({
            'uniprot': 'uniprot_picked',
            'sequence': 'sequence_picked',
        })
        .with_columns([
            # TODO: distinguish cited vs backcited vs doublecited
            pl.lit('uniprot cited picked').alias('sequence_source_picked')
        ])
    )
    gpt_df = (
        gpt_df.join(uniprot_picked, on=['pmid', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce(['uniprot', 'uniprot_picked']).alias('uniprot'),
            pl.coalesce(['sequence', 'sequence_picked']).alias('sequence'),
            pl.coalesce(['sequence_source', 'sequence_source_picked']).alias('sequence_source'),
        ]).drop('uniprot_picked', 'sequence_picked', 'sequence_source_picked')
    )

    ### add uniprot (old cited, pick-uniprot-prod1)
    uniprot_cited = (
        pl.read_parquet('data/thesaurus/enzymes/uniprots_cited.parquet')
        .filter(pl.col('uniprot').is_not_null() & pl.col('sequence').is_not_null())
        .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'uniprot', 'sequence'])
        .rename({
            'uniprot': 'uniprot_cited',
            'sequence': 'sequence_cited',
        })
        .with_columns([
            # TODO: distinguish cited vs backcited vs doublecited
            pl.lit('uniprot cited').alias('sequence_source_cited')
        ])
    )

    

    # fix dumb thing
    # enzyme_thesaurus = enzyme_thesaurus.with_columns([
    #     pl.when(pl.col('enzyme') == pl.col('enzyme_full'))
    #     .then(None)
    #     .otherwise(pl.col('enzyme_full')).alias('enzyme_full')
    # ])
    # join cited stuff, then immediately drop
    gpt_df = (
        gpt_df.join(uniprot_cited, on=['pmid', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce(['uniprot', 'uniprot_cited']).alias('uniprot'),
            pl.coalesce(['sequence', 'sequence_cited']).alias('sequence'),
            pl.coalesce(['sequence_source', 'sequence_source_cited']).alias('sequence_source'),
        ]).drop('uniprot_cited', 'sequence_cited', 'sequence_source_cited')
    )

    ### add uniprot (similar)
    # uniprot_similar = (
    #     pl.concat([
    #         pl.read_parquet('data/thesaurus/enzymes/uniprot_similar.parquet')
    #             .sort('total_similarity'),
    #         pl.read_parquet('data/thesaurus/enzymes/uniprot_similar_no_organism.parquet')
    #             .sort('similarity_organism'),
    #     ], how='diagonal')
    #     .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'uniprot'])
    #     .unique(['pmid', 'enzyme', 'enzyme_full', 'organism'], keep='first')
    #     .join(uniprot2seq, on='uniprot', how='left', validate='m:1') # adds sequence
    #     .rename({
    #         'uniprot': 'uniprot_similar',
    #         'sequence': 'sequence_similar',
    #     })
    #     .with_columns([
    #         pl.lit('uniprot cited').alias('sequence_source_similar')
    #     ])
    # )

    ### new uniprot (cited, similar)
    coalescables = ['sequence', 'sequence_source', 'max_enzyme_similarity', 'max_organism_similarity', 'total_similarity']
    u_coalescables = ['uniprot', *coalescables]
    uniprot_similar = (
        uniprot_conf
        .filter(
            (pl.col('max_enzyme_similarity') >= 90) 
            & ((pl.col('max_organism_similarity') >= 90)
                | pl.col('max_organism_similarity').is_null())
            & pl.col('sequence').is_not_null()
        ).with_columns(
            pl.lit('uniprot cited').alias('sequence_source')
        ).sort('total_similarity', descending=True)
        .unique(['canonical', 'enzyme', 'enzyme_full', 'organism'], keep='first')
        .select(['canonical', 'enzyme', 'enzyme_full', 'organism', *u_coalescables])
        .rename({x: f'{x}_similar' for x in u_coalescables})
    )
    gpt_df = (
        gpt_df.join(uniprot_similar, on=['canonical', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce([x, f'{x}_similar']).alias(x) for x in u_coalescables
        ]).drop([f'{x}_similar' for x in u_coalescables])
    )

    

    ##### PDB
    ### add pdb
    p_coalescables = ['pdb', *coalescables]
    pdb_picked = pl.read_parquet('data/thesaurus/enzymes/pdb_picked.parquet').filter(
        pl.col('pdb').is_not_null()
    )
    # pdb_similar = pl.read_parquet('data/thesaurus/enzymes/pdb_similar.parquet')
    # pdb_similar_no_organism = pl.read_parquet('data/thesaurus/enzymes/pdb_similar_no_organism.parquet')

    pdb2seq = (
        pl.read_parquet('data/enzymes/accessions/final/pdb.parquet')
        .select(['pdb', 'seq_can'])
        .filter(pl.col('seq_can').is_not_null())
        .unique('pdb')
    )
    gpt_to_pdb = (
        pl.concat([
            pdb_picked.select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'pdb']), 
            # pdb_similar.sort('total_similarity')
            #     .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'pdb']), 
            # pdb_similar_no_organism.sort('max_enzyme_similarity')
            #     .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'pdb']),
        ]).unique(['pmid', 'enzyme', 'enzyme_full', 'organism'], keep='first')
        .join(pdb2seq, left_on='pdb', right_on='pdb', how='left')
        .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'pdb', 'seq_can'])
        .with_columns([
            pl.lit('pdb cited picked').alias('sequence_source_pdb')
        ]).rename({
            'pdb': 'pdb_picked'
        })
    )

    

    # join pdb
    gpt_df = (
        gpt_df.join(gpt_to_pdb, on=['pmid', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, validate='m:1', how='left') # , 
        .with_columns([
            pl.coalesce(['pdb', 'pdb_picked']).alias('pdb'),
            pl.coalesce(['sequence', 'seq_can']).alias('sequence'),
            pl.coalesce(['sequence_source', 'sequence_source_pdb']).alias('sequence_source'),
        ]).drop('seq_can', 'sequence_source_pdb', 'pdb_picked')
    )

    pdb_similar = (
        pdb_conf
        .filter(
            (pl.col('max_enzyme_similarity') >= 90) 
            & ((pl.col('max_organism_similarity') >= 90)
                | pl.col('max_organism_similarity').is_null())
            & pl.col('sequence').is_not_null()
        ).with_columns(
            # multiplication of percentages
            # no organism present: confidence is 50%
            pl.lit('pdb cited').alias('sequence_source')
        ).sort('total_similarity', descending=True)
        .unique(['canonical', 'enzyme', 'enzyme_full', 'organism'], keep='first')
        .select(['canonical', 'enzyme', 'enzyme_full', 'organism', *p_coalescables])
        .rename({x: f'{x}_similar' for x in p_coalescables})
    )
    gpt_df = (
        gpt_df.join(pdb_similar, on=['canonical', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce([x, f'{x}_similar']).alias(x) for x in p_coalescables
        ]).drop([f'{x}_similar' for x in p_coalescables])
    )

    ##### NCBI
    ncbi2seq = (
        pl.read_parquet('data/enzymes/accessions/final/ncbi.parquet')
        .select(['ncbi', 'sequence'])
        .filter(pl.col('sequence').is_not_null())
        .unique('ncbi')
    ) # columns: ncbi, sequence
    ncbi_picked = (
        pl.read_parquet('data/thesaurus/enzymes/ncbi_picked.parquet')
        .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'ncbi'])
        .unique(['pmid', 'enzyme', 'enzyme_full', 'organism'], keep='first')
        .filter(pl.col('ncbi').is_not_null())
        .join(ncbi2seq, on='ncbi', how='left', validate='m:1') # adds sequence
        .rename({
            'ncbi': 'ncbi_picked',
            'sequence': 'sequence_picked',
        })
        .with_columns([
            pl.lit('ncbi cited picked').alias('sequence_source_picked')
        ])
    )
    gpt_df = (
        gpt_df.join(ncbi_picked, on=['pmid', 'enzyme', 'enzyme_full', 'organism'], join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce(['ncbi', 'ncbi_picked']).alias('ncbi'),
            pl.coalesce(['sequence', 'sequence_picked']).alias('sequence'),
            pl.coalesce(['sequence_source', 'sequence_source_picked']).alias('sequence_source'),
        ]).drop('ncbi_picked', 'sequence_picked', 'sequence_source_picked')
    )

    # ncbi_similar = (
    #     pl.concat([
    #         pl.read_parquet('data/thesaurus/enzymes/ncbi_similar.parquet')
    #         .sort('total_similarity'),
    #         pl.read_parquet('data/thesaurus/enzymes/ncbi_similar_no_organism.parquet')
    #         .sort('max_enzyme_similarity'),
    #     ], how='diagonal')
    #     .select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'ncbi'])
    #     .unique(['pmid', 'enzyme', 'enzyme_full', 'organism'], keep='first')
    #     .join(ncbi2seq, on='ncbi', how='left', validate='m:1') # adds sequence
    #     .rename({
    #         'ncbi': 'ncbi_similar',
    #         'sequence': 'sequence_similar',
    #     })
    #     .with_columns([
    #         pl.lit('ncbi cited').alias('sequence_source_similar')
    #     ])
    #     .unique(['pmid', 'enzyme', 'enzyme_full', 'organism'], keep='first')
    # )
    # gpt_df = (
    #     gpt_df.join(ncbi_similar, on=['pmid', 'enzyme', 'enzyme_full', 'organism'], join_nulls=True, how='left', validate='m:1')
    #     .with_columns([
    #         pl.coalesce(['ncbi', 'ncbi_similar']).alias('ncbi'),
    #         pl.coalesce(['sequence', 'sequence_similar']).alias('sequence'),
    #         pl.coalesce(['sequence_source', 'sequence_source_similar']).alias('sequence_source'),
    #     ]).drop('ncbi_similar', 'sequence_similar', 'sequence_source_similar')
    # )

    n_coalescables = ['ncbi', *coalescables]
    ncbi_similar = (
        ncbi_conf
        # pl.read_parquet('data/thesaurus/confident/ncbi.parquet')
        .filter(
            (pl.col('max_enzyme_similarity') >= 90) 
            & ((pl.col('max_organism_similarity') >= 90)
                | pl.col('max_organism_similarity').is_null())
            & pl.col('sequence').is_not_null()
        ).with_columns(
            pl.lit('ncbi cited').alias('sequence_source')
        ).sort('total_similarity', descending=True)
        .unique(['canonical', 'enzyme', 'enzyme_full', 'organism'], keep='first')
        .select(['canonical', 'enzyme', 'enzyme_full', 'organism', *n_coalescables])
        .rename({x: f'{x}_similar' for x in n_coalescables})
    )
    gpt_df = (
        gpt_df.join(ncbi_similar, on=['canonical', 'enzyme', 'enzyme_full', 'organism'], 
                    join_nulls=True, how='left', validate='m:1')
        .with_columns([
            pl.coalesce([x, f'{x}_similar']).alias(x) for x in n_coalescables
        ]).drop([f'{x}_similar' for x in n_coalescables])
    )



    
    ##### UNIPROT AGAIN
    ### add uniprot, searched
    s_coalescables = ['uniprot', 'sequence', 'sequence_source', 'max_enzyme_similarity', 'max_organism_similarity'] # , 'total_similarity']
    uniprot_searched = (
        pl.read_parquet('data/thesaurus/enzymes/uniprots_searched.parquet')
        .select(['query_enzyme', 'query_organism', 'uniprot', 'sequence', 'max_enzyme_similarity', 'max_organism_similarity']) # , 'enzyme_source'])
        .filter(pl.col('sequence').is_not_null())
        .with_columns([
            pl.lit('uniprot searched').alias('sequence_source')
        ])
        .rename({
            x: f'{x}_searched' for x in s_coalescables
        })
        .unique(['query_enzyme', 'query_organism'], keep='first')
    )
    # with searched, we need to join first by enzyme_full, then by enzyme
    gpt_df = (
        gpt_df.with_columns([
            pl_to_ascii(
                pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme')),
                lowercase=False
            )
            .str.replace_all(r"[!\"():\[\]^]", "")
            .alias('enzyme_ascii_fixed'),
            pl_fix_organism(pl.col('organism'))
            .str.replace_all(r"[!\"():\[\]^]", "")
            .alias('organism_ascii_fixed'),
        ])
        .join(uniprot_searched, left_on=['enzyme_ascii_fixed', 'organism_ascii_fixed'], right_on=['query_enzyme', 'query_organism'], how='left')

        .with_columns([
            pl.coalesce([x, f'{x}_searched']).alias(x) for x in s_coalescables
        ]).drop([f'{x}_searched' for x in s_coalescables])
        .join(uniprot_searched, left_on=['enzyme_full', 'organism'], right_on=['query_enzyme', 'query_organism'], how='left', validate='m:1')
        .with_columns([
            pl.coalesce([x, f'{x}_searched']).alias(x) for x in s_coalescables
        ]).drop([f'{x}_searched' for x in s_coalescables])
        .drop('enzyme_ascii_fixed', 'organism_ascii_fixed')
    )

    ### MERGE IN CONFIDENCES
    c_coalescables = ['max_enzyme_similarity', 'max_organism_similarity', 'total_similarity']
    


    uniprot_conf = (
        uniprot_conf.select(['canonical', 'enzyme', 'enzyme_full', 'organism', 'uniprot', *c_coalescables])
    ).sort('total_similarity', descending=True).unique(['canonical', 'enzyme', 'enzyme_full', 'organism', 'uniprot'], keep='first')
    gpt_df = gpt_df.join(uniprot_conf, on=['canonical', 'enzyme', 'enzyme_full', 'organism', 'uniprot'], 
                         how='left', join_nulls=True, validate='m:1', suffix='_confidences')
    gpt_df = gpt_df.with_columns([
        pl.coalesce([x, f'{x}_confidences']).alias(x) for x in c_coalescables
    ]).drop([f'{x}_confidences' for x in c_coalescables])

    pdb_conf = (
        pdb_conf.select(['canonical', 'enzyme', 'enzyme_full', 'organism', 'pdb', *c_coalescables])
    ).sort('total_similarity', descending=True).unique(['canonical', 'enzyme', 'enzyme_full', 'organism', 'pdb'], keep='first')
    gpt_df = gpt_df.join(pdb_conf, on=['canonical', 'enzyme', 'enzyme_full', 'organism', 'pdb'], 
                         how='left', join_nulls=True, validate='m:1', suffix='_confidences')
    gpt_df = gpt_df.with_columns([
        pl.coalesce([x, f'{x}_confidences']).alias(x) for x in c_coalescables
    ]).drop([f'{x}_confidences' for x in c_coalescables])
    
    ncbi_conf = ncbi_conf.select(
        ['canonical', 'enzyme', 'enzyme_full', 'organism', 'ncbi', *c_coalescables]
    ).sort('total_similarity', descending=True).unique(['canonical', 'enzyme', 'enzyme_full', 'organism', 'ncbi'], keep='first')
    gpt_df = gpt_df.join(ncbi_conf, on=['canonical', 'enzyme', 'enzyme_full', 'organism', 'ncbi'], 
                         how='left', join_nulls=True, validate='m:1', suffix='_confidences')
    gpt_df = gpt_df.with_columns([
        pl.coalesce([x, f'{x}_confidences']).alias(x) for x in c_coalescables
    ]).drop([f'{x}_confidences' for x in c_coalescables])


    # print(gpt_df)
    return gpt_df

def add_flags(gpt_df: pl.DataFrame) -> pl.DataFrame:
    """Add flags that indicate possible issues with filtering"""

    gpt_df = gpt_df.with_columns([
        (
            pl.col('kcat').str.contains('10^') | pl.col('kcat').str.contains('10^')
        ).alias('flags.scientific_notation'),
        (
            (pl.col('substrate').str.len_chars() <= 2) 
            | (pl.col('substrate').str.contains('^[0-9]+$'))
            & pl.col('substrate_full').is_null()
        ).alias('flags.bad_substrate'),
    ])

def ec_diversity():
    ecs = load_ecs()
    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet').filter(pl.col('km').str.contains('\d')).select(['enzyme', 'enzyme_full'])
    gpt_df = gpt_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
    gpt_df = gpt_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')
    df = gpt_df.with_columns(pl.col('enzyme_ecs').list.concat(pl.col('enzyme_ecs_full')).list.unique().alias('ecs_all'))
    df = df.select('ecs_all').explode('ecs_all').unique()
    print(df) 
    # 3071 ecs for kcat
    # 3739 ecs for km

def cid_diversity():
    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet')
    add_identifiers(gpt_df)
    df = gpt_df.with_columns(pl.col('cid').list.concat(pl.col('cid_full')).list.unique().alias('cids_all'))
    df = df.select('cids_all').explode('cids_all').unique()
    print(df)
    df.write_parquet('data/thesaurus/cids/latest_cids.parquet')

def step4_main(
    gpt_df: pl.DataFrame,
    subs_df: pl.DataFrame,
    include_enzyme_sequences: bool = False,
):
    ### Step 1: Generate thesaurus    
    # so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
    # base_df = pl.read_csv('data/humaneval/rumble/rumble_20241219.csv', schema_overrides=so)
    # abbr_df = pl.read_parquet('data/thesaurus/abbr/beluga-abbrs-4ostruct_20241213.parquet')

    # thesaurus = script_generate_thesaurus(gpt_df, base_df, abbr_df)
    # thesaurus.write_parquet('data/thesaurus/substrate/latest_substrate_thesaurus.parquet')
    # exit(0)

    ### Step 2: Add identifiers
    
    df = add_identifiers(gpt_df, subs_df=subs_df)

    if include_enzyme_sequences:
        # add enzyme equences to the df
        df = add_enzyme_sequences(df)



    return df

    # relaxing perfect requirements: 95679 --> 96101
    # sequence confidence score
    # - multiply similarity as the confidence score
    # - confidence score helps with different communities that demand different levels of accuracy
    # - enzyme name, enzyme organism, 
    # confidence for substrate: we demand exact matches
