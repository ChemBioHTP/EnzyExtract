import polars as pl

manifest = pl.read_parquet('data/manifest.parquet')

xml_manifest = pl.read_parquet('data/xml_manifest.parquet')
xml_manifest = xml_manifest.select(['pmid', 'canonical'])
pmid2canonical = manifest.select(['pmid', 'canonical'])

df1 = pl.read_parquet('data/valid/_valid_bucket-rebuilt.parquet')
# df1 = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')

df2 = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
# df2 = pl.read_parquet('data/valid/_valid_bucket-rebuilt.parquet/')
df2 = df2.filter(
    ~pl.col('pmid').cast(pl.Utf8).is_in(df1.select('pmid').cast(pl.Utf8))
)

df = pl.concat([df1, df2])


df3 = pl.read_parquet('data/valid/_valid_apatch-rebuilt.parquet')
df3 = df3.filter(
    ~pl.col('pmid').cast(pl.Utf8).is_in(df.select('pmid').cast(pl.Utf8))
)

df = pl.concat([df, df3])
kcat = df.filter(
    pl.col('kcat').str.contains(r'\d+').cast(pl.Boolean)
) # 220231
# print(kcat)
df = df.write_parquet('data/valid/_valid_everything.parquet')
