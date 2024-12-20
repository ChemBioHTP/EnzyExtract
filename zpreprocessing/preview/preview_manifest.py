import polars as pl

df = pl.read_parquet('zpreprocessing/data/manifest.parquet')

df = df.with_columns([
    pl.col("filepath").str.extract_groups("D:/papers/(\w+/\w+)").alias("levels")
]).unnest("levels")

# report % kinetic in column 1
# kinetic_df = df.group_by("1").agg(pl.col("kinetic").cast(pl.Int32).sum() / pl.col("kinetic").count())
unprocessed_df = df.group_by("1").agg(
    ((pl.col("readable") & ~pl.col("apogee_processed")).cast(pl.Int32).sum() / pl.col("readable").count()).alias("unprocessed")
)
print("Unprocessed: ")
with pl.Config(tbl_rows=-1):
    print(unprocessed_df.filter(
        pl.col("unprocessed") > 0.1
    ).sort("unprocessed", descending=True))
# unprocessed:
# topoff/open, wos/remote_all, wos_local_shim, wos/jbc

# has low amount of skipped:
# brenda/scihub, brenda/open, brenda/hindawi, brenda_asm, brenda/pnas

# actually have processed: 35591/60567 papers

# report % kinetic

kinetic_df = df.filter(
    pl.col("apogee_processed")
).group_by("1").agg(
    (pl.col("kinetic").cast(pl.Int32).sum() / pl.col("kinetic").count()).alias("pct_kinetic")
)
print("Kinetic: ")
with pl.Config(tbl_rows=-1):
    print(kinetic_df.sort("pct_kinetic", descending=True))