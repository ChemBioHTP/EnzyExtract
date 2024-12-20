import polars as pl

so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
base_df = pl.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', schema_overrides=so)

want_substrates = base_df.select(['substrate', 'substrate_full', 'substrate_2'])
cids = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Synonym-filtered.parquet')
cids = cids.filter(
    pl.col('name').str.to_lowercase().is_in(brenda_names)
)
# Add the headers as a new row

# Rename the columns

brendas = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')

brenda_names = set(brendas['name'].str.to_lowercase().to_list())
print(len(brenda_names)) # 238814


print(cids.shape) # (64272, 2)

print(cids.head(10))