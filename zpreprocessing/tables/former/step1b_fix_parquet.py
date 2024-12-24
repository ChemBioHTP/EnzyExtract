import polars as pl
df = pl.read_parquet('zpreprocessing/data/pdf_tables.parquet')
df = df.rename({"filename": "filepath"}).with_columns([
    pl.col("filepath").map_elements(lambda s: s.rsplit('/', 1)[1], return_dtype=pl.Utf8).alias("filename"),
    # pl.col("filepath").map_elements(lambda s: s.rsplit('/', 1)[0], return_dtype=pl.Utf8).alias("filepath")
])
df.drop_in_place('filepath')
# old filepaths are useless
df.write_parquet('zpreprocessing/data/pdf_tables.parquet')