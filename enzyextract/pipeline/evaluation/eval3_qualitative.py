import polars as pl


from enzyextract.pipeline.step5_generate_identifiers import add_identifiers, load_ecs

def _ec_diversity():
    ecs = load_ecs()
    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet').filter(pl.col('km').str.contains('\d')).select(['enzyme', 'enzyme_full'])
    gpt_df = gpt_df.join(ecs, left_on='enzyme', right_on='alias', how='left')
    gpt_df = gpt_df.join(ecs, left_on='enzyme_full', right_on='alias', how='left', suffix='_full')
    df = gpt_df.with_columns(pl.col('enzyme_ecs').list.concat(pl.col('enzyme_ecs_full')).list.unique().alias('ecs_all'))
    df = df.select('ecs_all').explode('ecs_all').unique()
    print(df) 
    # 3071 ecs for kcat
    # 3739 ecs for km

def _cid_diversity():
    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet')
    add_identifiers(gpt_df)
    df = gpt_df.with_columns(pl.col('cid').list.concat(pl.col('cid_full')).list.unique().alias('cids_all'))
    df = df.select('cids_all').explode('cids_all').unique()
    print(df)
