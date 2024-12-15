from rapidfuzz import fuzz
import polars as pl

from enzyextract.equivalence.convert_ec import add_ecs

def compute_string_similarities(df: pl.DataFrame, to_ec_df=None, brenda_mode=None):
    
    if brenda_mode is None:
        brenda_mode = 'ec_2' in df.columns and df['ec_2'].drop_nulls().n_unique() > 0
    

    df = df.with_columns([
        pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme')).alias('enzyme_preferred')
            if 'enzyme_full' in df.columns else 
            pl.col('enzyme').alias('enzyme_preferred'),
        pl.coalesce(pl.col('substrate_full'), pl.col('substrate')).alias('substrate_preferred')
            if 'substrate_full' in df.columns else
            pl.col('substrate').alias('substrate_preferred')
    ])

    df = df.with_columns([
        pl.struct(['enzyme_preferred', 'enzyme_2'])
            .map_elements(lambda x: None if (x['enzyme_preferred'] is None or x['enzyme_2'] is None) else 
                          fuzz.ratio(str(x['enzyme_preferred']), str(x['enzyme_2'])) / 100.0, return_dtype=pl.Float64)
            .alias('enzyme_similarity'),
        pl.struct(['substrate_preferred', 'substrate_2'])
            .map_elements(lambda x: None if (x['substrate_preferred'] is None or x['substrate_2'] is None) else 
                          fuzz.ratio(str(x['substrate_preferred']), str(x['substrate_2'])) / 100.0, return_dtype=pl.Float64)
            .alias('substrate_similarity')
    ])

    # consider >60% similarity to be a match
    df = df.with_columns([
        (pl.col('enzyme_similarity') > 0.6).alias('enzyme_match'),
        (pl.col('substrate_similarity') > 0.6).alias('substrate_match')
    ])
    # use ec match to create viable_ecs

    # if runeem_df is the GT, then viable_ecs_2 will be created.

    # if runeem_df is the analyte, then drop viable_ecs.
    if 'viable_ecs' in df.columns:
        df.drop_in_place('viable_ecs')
    # df = df.join(to_ec_df, left_on=['enzyme'], right_on=['alias'], how='left') # creates viable_ecs for the LHS
    df = add_ecs(df, to_ec_df)
    if 'enzyme_full' in df.columns:
        df = df.join(to_ec_df, left_on=['enzyme_full'], right_on=['alias'], how='left', suffix='_full') # creates viable_ecs_right for the LHS
        df = df.with_columns([
            pl.coalesce(pl.col('viable_ecs_full'), pl.col('viable_ecs')).alias('viable_ecs') # prefer the full enzyme name EC, if available, otherwise fallback on the short name
        ])
        df.drop_in_place('viable_ecs_full')


    # print(df.schema)

    if brenda_mode:
        # cmopare ours.viable_ecs vs brenda.ec_2
        df = df.with_columns([
            pl.when(pl.col('enzyme_match').is_not_null())
            .then(pl.col('viable_ecs').list.contains(pl.col('ec_2')))
            .otherwise(pl.lit(None)).alias('ec_match')
        ])
    else:
        # compare ours.viable_ecs vs runeem_ec.viable_ecs_2
        df = df.with_columns([
            pl.col('viable_ecs_2').str.split('; ').alias('viable_ecs_2'),
        ])

        df = df.with_columns([
            pl.when(pl.col('enzyme_match').is_not_null())
            .then(pl.col('viable_ecs').list.set_intersection(pl.col('viable_ecs_2')).list.len() > 0)
            .otherwise(pl.lit(None)).alias('ec_match')
        ])
        
    
    df = df.with_columns([
        (pl.col('enzyme_match') | pl.col('ec_match')).alias('enzyme_or_ec_match')
    ])
    return df