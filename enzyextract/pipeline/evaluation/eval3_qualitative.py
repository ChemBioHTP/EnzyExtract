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

def preview_height():
    df = pl.read_parquet('data/export/TheData_kcat.parquet')
    df = df.filter(
        # (pl.col('enzyme').is_not_null() | pl.col('enzyme_full').is_not_null()) &
        # (pl.col('substrate').is_not_null() | pl.col('substrate_full').is_not_null())
        # & pl.col('kcat_value').is_not_null()
    )
    print(df)

def preview_differences_in():
    col = 'km'

    against = 'brenda' # =2

    working = 'rumble' # =1

    readme = f'data/metrics/{against}/{against}_{working}.parquet'
    print("Reading", readme)
    matched_view = pl.read_parquet(readme)

    matched_view = matched_view.filter(
        pl.col('same_enzyme') & pl.col('same_substrate')
    )
    if col == 'kcat':
        matched_view = matched_view.filter(
            pl.col('kcat_diff') > 1.1
        )
    elif col == 'km':
        matched_view = matched_view.filter(
            pl.col('km_diff') > 1.1
        )
    view = matched_view.select('pmid', 'objective', 'comments_2',  'substrate_1', 'substrate_2', 'kcat_1', 'kcat_2', 'kcat_diff', 'km_1', 'km_2', 'km_diff', 'clean_mutant_1', 'clean_mutant_2')
    pass

if __name__ == '__main__':
    # _ec_diversity()
    # _cid_diversity()
    preview_height()
    # preview_differences_in()
    # raise NotImplementedError("This script is only an example.")
    # print("Run the following command to see the differences in kcat and km:")
    # print("python -m enzyextract.pipeline.evaluation.eval3_qualitative")