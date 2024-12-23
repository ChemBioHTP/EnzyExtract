import polars as pl
def add_columns_es_preferred(df):
    """
    Put columns 'enzyme_preferred', 'enzyme_preferred_2', 
    'substrate_preferred', 'substrate_preferred_2' in the dataframe.
    """
    if 'enzyme_preferred' not in df.columns:
        df = df.with_columns([
            (pl.coalesce(['enzyme_full', 'enzyme']) if 'enzyme_full' in df.columns else pl.col('enzyme'))
                .alias('enzyme_preferred'),
        ])
    if 'enzyme_preferred_2' not in df.columns:
        df = df.with_columns([
            (pl.coalesce(['enzyme_full_2', 'enzyme_2']) if 'enzyme_full_2' in df.columns else pl.col('enzyme_2'))
                .alias('enzyme_preferred_2'),
        ])
    if 'substrate_preferred' not in df.columns:
        df = df.with_columns([
            (pl.coalesce(['substrate_full', 'substrate']) if 'substrate_full' in df.columns else pl.col('substrate'))
                .alias('substrate_preferred'), 
        ])
    if 'substrate_preferred_2' not in df.columns:
        df = df.with_columns([
            (pl.coalesce(['substrate_full_2', 'substrate_2']) if 'substrate_full_2' in df.columns else pl.col('substrate_2'))
                .alias('substrate_preferred_2'),
        ])
    return df