# report coverage of sabiork pmids
import polars as pl


print("SabioRK pmids:")
df = pl.read_parquet('data/external/sabiork/sabiork.parquet')
print(df.select('PubMedID').n_unique())

manifest = pl.read_parquet('data/manifest.parquet')

df = df.select(['PubMedID']).unique()
df = df.cast({'PubMedID': pl.Utf8})
print("Num covered already:")
manifest = manifest.join(df, left_on='canonical', right_on='PubMedID', how='inner')
print(manifest.select('canonical').n_unique())
# 1113 / 3123