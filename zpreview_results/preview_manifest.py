import polars as pl

manifest = pl.read_parquet('data/manifest.parquet')

df = manifest.with_columns([
    pl.col("filepath").str.extract_groups("D:/papers/(\w+/\w+)").alias("levels")
]).unnest("levels")

# subtract rekcat?
# indeed, rekcat makes up all of the unprocessed papers in BRENDA.
rekcat_pmids = pl.read_parquet("data/pmids/brenda_rekcat.parquet")['pmid']

# report % kinetic in column 1
# kinetic_df = df.group_by("1").agg(pl.col("apogee_kinetic").cast(pl.Int32).sum() / pl.col("apogee_kinetic").count())
unprocessed_df = df.group_by("1").agg(
    ((pl.col("readable") 
      & ~pl.col("apogee_processed")
      & ~pl.col("canonical").is_in(rekcat_pmids)
      ).cast(pl.Int32).sum() / pl.col("readable").count()).alias("unprocessed")
)
print("Unprocessed: ")
with pl.Config(tbl_rows=-1):
    print(unprocessed_df
    .filter(pl.col("unprocessed") > 0.1)
    .sort("unprocessed", descending=True))
# unprocessed:
# topoff/open, wos/remote_all, wos_local_shim, wos/jbc

# has low amount of skipped:
# brenda/scihub, brenda/open, brenda/hindawi, brenda_asm, brenda/pnas

# actually have processed: 35591/60567 papers

# report % kinetic

# kinetic_df = df.filter(
#     pl.col("apogee_processed")
# ).group_by("toplevel").agg(
#     (pl.col("apogee_kinetic").cast(pl.Int32).sum() / pl.col("apogee_kinetic").count()).alias("pct_kinetic")
# )
# print("Kinetic: ")
# with pl.Config(tbl_rows=-1):
#     pass
    # print(kinetic_df.sort("pct_kinetic", descending=True))


# see how well certain keywords are able to cover kinetic papers

# check gmft coverage for wos/remote_all
df = manifest.filter(
    pl.col("filepath").str.contains("wos/remote_all")
)
summary = df.group_by("filepath").agg(
    (pl.col("had_gmft").cast(pl.Int32).sum() / pl.col("had_gmft").count()).alias("pct_gmft")
)
print("GMFT coverage: ")
with pl.Config(tbl_rows=-1):
    print(summary.sort("pct_gmft", descending=True))