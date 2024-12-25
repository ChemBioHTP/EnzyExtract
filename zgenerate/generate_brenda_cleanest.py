import polars as pl

df = pl.read_parquet('data/brenda/brenda_kcat_v3.parquet')

cleanest_df = df.filter(
    pl.col('comments').is_null()
    | pl.col('good_reference_ids').is_not_null()
)
cleanest_df.write_parquet('data/brenda/brenda_kcat_cleanest.parquet')