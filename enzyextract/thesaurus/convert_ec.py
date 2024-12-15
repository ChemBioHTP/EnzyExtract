import polars as pl

def add_ecs(df: pl.DataFrame, to_ec_df: pl.DataFrame, name_cols=None, write_col=None, *, 
            do_lowercase=True):
    """
    Add EC numbers, 


    """

    if name_cols is None:
        name_cols = ['enzyme_full', 'enzyme']
        name_cols = [col for col in name_cols if col in df.columns]

    if write_col is None:
        write_col = 'viable_ecs'
        if write_col in df.columns:
            write_col = 'viable_ecs_2'
        if write_col in df.columns:
            raise ValueError("viable_ecs_x already exists in df")
    
    # perform ec merge
    # df = df.with_columns([
    #     pl.coalesce([pl.col(col) for col in name_cols])
    #         .alias('enzyme_preferred')
    # ])
    # df = df.join(to_ec_df, left_on="enzyme_preferred", right_on="alias", how="left")
    to_ec_df = to_ec_df.rename({'viable_ecs': '_viable_ecs_temp'})

    df = df.with_columns([
        pl.lit(None).alias(write_col)
    ])

    # perform a join for each, and then coalesce the results
    for col in name_cols:
        
        df = df.join(to_ec_df, left_on=col, right_on='alias', how='left')
        df = df.with_columns([
            # prefer the full enzyme name EC, if available, otherwise fallback on the short name
            pl.coalesce(pl.col(write_col), pl.col('_viable_ecs_temp')).alias(write_col)
        ])
        df.drop_in_place('_viable_ecs_temp')
    

    # if there are unmatched, then try a case-insensitive run

    if do_lowercase:
        to_ec_lower = to_ec_df.with_columns([
            pl.col("alias").str.to_lowercase().alias("alias_lower"),
        ])
        # re-groupby
        to_ec_lower = to_ec_lower.explode("_viable_ecs_temp").group_by("alias_lower").agg(pl.col("_viable_ecs_temp"))

        for col in name_cols:
            df = df.join(to_ec_df, left_on=col, right_on='alias', how='left')
            df = df.with_columns([
                # prefer the full enzyme name EC, if available, otherwise fallback on the short name
                pl.coalesce(pl.col(write_col), pl.col('_viable_ecs_temp')).alias(write_col)
            ])
            df.drop_in_place('_viable_ecs_temp')
    
    return df