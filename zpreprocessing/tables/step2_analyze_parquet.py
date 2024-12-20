import os
import polars as pl

tables_df = pl.scan_parquet('zpreprocessing/data/pdf_tables.parquet').select("filename").rename({"filename": "filepath"}).unique().collect()

# incomplete: topoff
tables_df = tables_df.with_columns([
    pl.col("filepath").map_elements(lambda s: s.rsplit('/', 1)[1], return_dtype=pl.Utf8).alias("filename"),
    pl.col("filepath").map_elements(lambda s: s.rsplit('/', 1)[0], return_dtype=pl.Utf8).alias("filepath")
]).with_columns([
    pl.col("filepath")
        .str.strip_chars_end('/')
        .str.replace("^D:/", "D:/papers/")
        .replace("D:/papers/scratch/open_remote", "D:/papers/scratch/open")
    .alias("filepath"),
])

# get summary statistics
# summary = df.group_by("filepath").agg([
#     pl.col("filepath").count().alias("count"),
#     # pl.col("filename").n_unique().alias("n_unique"),
# ]).sort("count", descending=True)
table_filenames = tables_df.select("filename").unique()

# now, get true pdf counts of each of those locations
manifest_df = pl.read_parquet('zpreprocessing/data/manifest.parquet')

manifest_df = manifest_df.with_columns([
    pl.col("filename").is_in(table_filenames).alias("had_gmft"),
])

total_counts = manifest_df.group_by("filepath").agg([
    # pl.col("filename").count().alias("pdf_count"),
    pl.col("readable").replace(False, None).count().alias("readable_count"),
    pl.col("had_gmft").replace(False, None).count().alias("table_count"),
    pl.col("filename").filter(pl.col("had_gmft")).max().alias("latest_table")
])

total_counts = total_counts.with_columns([
    (pl.col("table_count") / pl.col("readable_count")).alias("pct_table"),
]).sort("filepath") # , descending=True)

# join the two
# summary = total_counts.join(summary, on='filepath', how='left')

# we seem to have missed: 
# wos/open
# wos/wiley
# wos/remote_all, 
# wos/asm
# wos/hindawi
# topoff/open
# 

with pl.Config(tbl_rows=-1):
    print(total_counts)

