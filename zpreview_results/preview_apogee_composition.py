import polars as pl

so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
apogee_nonbrenda = pl.read_csv('data/_compiled/apogee-nonbrenda.tsv', schema_overrides=so, separator='\t')

apogee_mysterious = pl.read_parquet('data/_compiled/apogee_all.parquet')

rekcat = pl.read_parquet('data/pmids/brenda_rekcat.parquet').select(['pmid']).unique()

a1 = rekcat.join(apogee_nonbrenda, on='pmid', how='inner') # nothing
# print(a1) 0 rows

a2 = apogee_mysterious.join(rekcat, on='pmid', how='inner') # rekcat is 2822 rows
# print(a2) 2822 rows

# CONCLUSION: rekcat is stored in apogee_mysterious

# get pmids present in apogee_nonbrenda and not in apogee_mysterious
# a3 = apogee_nonbrenda.join(apogee_mysterious, on='pmid', how='anti')
# print(a3) # 49453 rows

# now look at the ones in manifest
# a3pmids = a3.select(['pmid']).unique()
manifest = pl.read_parquet('data/manifest.parquet')
# manifest = manifest.with_columns([
#     pl.col('filename').str.replace('.pdf$', '').alias('filepmid')
# ])
manifest4nonbrenda = manifest.filter(
    pl.col('pmid').is_in(apogee_nonbrenda['pmid'])
    & ~pl.col('pmid').is_in(apogee_mysterious['pmid'])
)
# print(manifest) 
# 49453 rows
# from wos/local_shim

# now take a look at the fileroot distribution
summary = manifest4nonbrenda.group_by('fileroot').agg(pl.col('filename').count().alias('count'))
print(summary)

# exclusive to nonbrenda:
# ┌──────────────────────────┬───────┐
# │ fileroot                 ┆ count │
# ╞══════════════════════════╪═══════╡
# │ D:/papers/wos/local_shim ┆ 3     │
# │ D:/papers/wos/remote_all ┆ 116   │
# │ D:/papers/wos/open       ┆ 1     │
# │ D:/papers/scratch/wiley  ┆ 1     │
# │ D:/papers/scratch/open   ┆ 5433  │
# └──────────────────────────┴───────┘

manifest4mysterious = manifest.filter(
    pl.col('filepmid').is_in(apogee_mysterious['pmid'])
    & ~pl.col('filepmid').is_in(apogee_nonbrenda['pmid'])
)

summary = manifest4mysterious.group_by('fileroot').agg(pl.col('filename').count().alias('count'))
print(summary)
# CONCLUSION: 

# exclusive to mysterious:
# ┌──────────────────────────┬───────┐
# │ fileroot                 ┆ count │
# ╞══════════════════════════╪═══════╡
# │ D:/papers/brenda/jbc     ┆ 1884  │
# │ D:/papers/brenda/wiley   ┆ 1761  │
# │ D:/papers/brenda/scihub  ┆ 3513  │
# │ D:/papers/brenda/pnas    ┆ 86    │
# │ D:/papers/brenda/asm     ┆ 626   │
# │ D:/papers/wos/local_shim ┆ 119   │
# │ D:/papers/brenda/open    ┆ 4016  │
# └──────────────────────────┴───────┘

# Mysterious has: BRENDA and some of wos/local_shim
# Mysterious also has: 
# Nonbrenda has: scratch/open and some of wos/remote_all
