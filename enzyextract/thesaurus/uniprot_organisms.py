import polars as pl

from enzyextract.thesaurus.organism_patterns import organism_patterns

def load_uniprot_names(space_after_common=True):
    """
    Returns a dataframe of organism names from UniProt. Columns:
    - organism: name of an organism
    - organism_type: full, common, short genus
    """
    uniprotdf = pl.read_parquet('data/thesaurus/organism/uniprot_organism.parquet')
    # full names
    uniprotdf_full = uniprotdf.select(
        'organism',
    )
    # for bacteria, add the strain-less variant
    uniprotdf_strainless = uniprotdf_full.with_columns(
        pl.col('organism').str.replace(r' \(strain .*\)', '')
    )
    uniprotdf_full = pl.concat([
        uniprotdf_full,
        uniprotdf_strainless
    ]).unique()
    
    # common names
    # these common names are too common
    uniprotdf_common = uniprotdf.select('organism_common').unique().drop_nulls().filter(
        (pl.col('organism_common').str.len_chars() > 4) # exclude super short names
        # # permit "human-readable" names
        | (pl.col('organism_common').str.contains(r'^[A-Z][a-z]+$')) 
    ).filter(
        ~pl.col('organism_common').is_in(['Li', 'Cat']) # FP are too common with these
    )
    
    # short genus
    uniprotdf_sg = uniprotdf.select('organism').filter(
        pl.col('organism').str.contains(r'^[A-Za-z][a-z]* [A-Za-z][a-z][a-z]+( \(strain .*\))?$')
    ).with_columns(
        pl.col('organism').str.replace(r'([A-Za-z])([a-z]*) (.*)', r'$1. $3')
    ).unique()

    # combine all these names
    uniprotdf_names = pl.concat([
        uniprotdf_full.select('organism', pl.lit('full').alias('organism_type')),
        uniprotdf_common.select(
            pl.col('organism_common').alias('organism'),
            pl.lit('common').alias('organism_type')
        ),
        uniprotdf_sg.select('organism', pl.lit('short genus').alias('organism_type')),
    ]).unique()

    # manual names
    manual_names = pl.DataFrame({
        'organism': [k for k in organism_patterns.keys() if k[0].isupper()],
    }).with_columns(
        pl.lit('manual').alias('organism_type')
    )
    manual_names = manual_names.join(
        uniprotdf_names,
        how='anti',
        on='organism'
    )


    all_names = pl.concat([
        uniprotdf_names,
        manual_names
    ]).sort(
        pl.col('organism').str.len_chars(),
        descending=False
    )
    if space_after_common:
        # intended to help with regex and word boundaries
        all_names = all_names.with_columns(
            pl.when(
                (pl.col('organism_type') == 'common')
                | (pl.col('organism_type') == 'manual')
            ).then(
                (' ' + pl.col('organism') + ' ').alias('organism')
            ).otherwise(
                pl.col('organism')
            )
        )
    # outliers



    return all_names