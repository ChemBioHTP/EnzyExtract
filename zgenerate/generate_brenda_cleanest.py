import polars as pl

def gen_cleanest():
    df = pl.read_parquet('data/brenda/brenda_kcat_v3.parquet')

    cleanest_df = df.filter(
        pl.col('comments').is_null()
        | pl.col('good_reference_ids').is_not_null()
    )
    cleanest_df.write_parquet('data/brenda/brenda_kcat_cleanest.parquet')

def gen_brenda_kcat():
    df = pl.read_parquet('data/brenda/brenda_kcat_v3.parquet')

    substrate2inchi = pl.read_parquet('data/substrates/brenda_inchi_all.parquet').filter(
        pl.col('inchi') != '-'
    ).unique('name')

    df = df.filter(
        pl.col('turnover_number').is_not_null()
        & pl.col('organism_name').is_not_null()
    )
    df = df.join(substrate2inchi, left_on='substrate', right_on='name', how='inner')
    print(df) # 66702

if __name__ == '__main__':
    gen_brenda_kcat()
    # gen_cleanest()