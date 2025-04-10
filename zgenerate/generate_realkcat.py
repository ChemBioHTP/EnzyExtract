import polars as pl
import polars.selectors as cs
def _convert_realkcat(df: pl.DataFrame):
    
    united = (
        (pl.col('PubMedID_x') == pl.col('PubMedID_y'))
        # & (~pl.col('PubMedID_x').str.contains(';'))
        & (pl.col('UniprotID_x') == pl.col('UniprotID_y'))
        # & (pl.col('Source_x') == pl.col('Source_y'))
        # only keep rows where both kcat and km are from the same pmid
    )

    # 3604/27176
    bifurcated = df.filter(~united)

    ### Filter out km and kcat that come from different pmids
    km_columns = ['km', *[x for x in df.columns if x.endswith('_x')]]
    kcat_columns = ['kcat', *[x for x in df.columns if x.endswith('_y')]]
    km_only = bifurcated.select(cs.exclude(kcat_columns))
    kcat_only = bifurcated.select(cs.exclude(km_columns))

    df = pl.concat(
        [df.filter(united),
        km_only,
        kcat_only],
        how='diagonal_relaxed'
    )

    cleaner = (
        (pl.col('PubMedID_x').is_null() |
            ~pl.col('PubMedID_x').str.contains(';'))
        & (~pl.col('PubMedID_y').str.contains(';') |
            pl.col('PubMedID_y').is_null())
    )
    unclean = df.filter(~cleaner)
    df = df.filter(cleaner)

    df = df.with_columns([
        pl.col('kcat_Unit').str.replace('s^(-1)', 's^-1', literal=True)
    ]).with_columns([
        # pl.col('PubMedID_x').str.split(';'),
        # pl.col('PubMedID_y').str.split(';'),
        (pl.col('kcat_Value').cast(pl.Utf8) + ' ' + pl.col('kcat_Unit')).alias('kcat'),
        (pl.col('km_Value').cast(pl.Utf8) + ' ' + pl.col('km_Unit')).alias('km'),
        pl.lit(None).cast(pl.Utf8).alias('substrate_full'),
        pl.lit(None).cast(pl.Utf8).alias('enzyme'),
        pl.lit(None).cast(pl.Utf8).alias('enzyme_full'),
        pl.lit(None).cast(pl.Utf8).alias('pH'),
        pl.lit(None).cast(pl.Utf8).alias('temperature'),
        pl.concat_list(pl.col('ECNumber')).alias('enzyme_ecs')
    ])

    renames = {
        # 'ECNumber': 'ec'
        'Organism': 'organism',
        # 'EnzymeType': 'mutant',
        'mutantSites_x': 'mutant',

        'Canonical SMILES': 'smiles',
        'Substrate': 'substrate',
        # 'sequence': 'sequence',
        # 'kcat_Value': 
        'PubMedID_x': 'pmid',
        # 'PubMedID_y'
    }

    drops = {

    }

    df = df.rename(renames)
    return df

def main_convert_realkcat():
    so = {
        'PubMedID_x': pl.Utf8,
        'PubMedID_y': pl.Utf8
    }
    df = pl.read_csv('data/external/realkcat/kcat_km_entries.csv', schema_overrides=so)
    df = _convert_realkcat(df)
    df.write_parquet('data/external/realkcat/realkcat.parquet')

if __name__ == '__main__':
    main_convert_realkcat()