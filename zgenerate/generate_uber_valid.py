import polars as pl
from enzyextract.utils.pmid_doi_convert import find_canonical
manifest = pl.read_parquet('data/manifest.parquet')

xml_manifest = pl.read_parquet('data/xml_manifest.parquet')
xml_manifest = xml_manifest.select(['pmid', 'canonical'])
# pmid2canonical = manifest.select(['filename', 'pmid'])

df = pl.read_parquet('data/valid/_valid_bucket-rebuilt.parquet')
# df1 = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')

df_strange = df.filter(
    pl.col('pmid').str.contains('^\d+_\d+')
)
assert df_strange.height == 0

df = df.join(xml_manifest, left_on='pmid', right_on='pmid', how='left')
df = df.with_columns([
    pl.col('pmid').map_elements(lambda x: find_canonical(x, 'SILLY'), return_dtype=pl.Utf8).alias('canonical')
])

# these only apply to pdfs
bad_pmids = manifest.filter(
    ~pl.col('readable')
    | pl.col('bad_ocr')
).select('pmid')

apogee = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
apogee_bad = pl.read_parquet('data/pmids/apogee_prompt_regurgitate.parquet')['pmid']


# fix this dumb error
apogee = apogee.with_columns([
    pl.col('pmid').str.replace(r'^1_(.*)$', r'$1').alias('pmid')
])

apogee = apogee.with_columns([
    pl.col('pmid').map_elements(lambda x: find_canonical(x, 'SILLY'), return_dtype=pl.Utf8).alias('canonical')
])
apogee = apogee.filter(
    ~pl.col('pmid').is_in(df.select('pmid'))
    & ~pl.col('canonical').is_in(df.select('canonical'))
    & ~pl.col('pmid').is_in(bad_pmids)
    & ~pl.col('canonical').is_in(bad_pmids)
    & ~pl.col('pmid').is_in(apogee_bad)
    & ~pl.col('canonical').is_in(apogee_bad)
)


df = pl.concat([df, apogee])

df_strange = apogee.filter(
    pl.col('pmid').str.contains('^1_\d+')
)


assert df_strange.height == 0

apatch = pl.read_parquet('data/valid/_valid_apatch-rebuilt.parquet')
apatch_bad = pl.read_parquet('data/pmids/apatch_prompt_regurgitate.parquet')['pmid']
apatch = apatch.with_columns([
    pl.col('pmid').map_elements(lambda x: find_canonical(x, 'SILLY'), return_dtype=pl.Utf8).alias('canonical')
])
apatch = apatch.filter(
    ~pl.col('pmid').is_in(df.select('pmid'))
    & ~pl.col('canonical').is_in(df.select('canonical'))
    & ~pl.col('pmid').is_in(bad_pmids)
    & ~pl.col('canonical').is_in(bad_pmids)
    & ~pl.col('pmid').is_in(apatch_bad)
    & ~pl.col('canonical').is_in(apatch_bad)
)

df = pl.concat([df, apatch])

df_strange = df.filter(
    pl.col('pmid').str.contains('^\d+_\d+')
)
assert df_strange.height == 0

# final summary
kcat = df.filter(
    pl.col('kcat').str.contains(r'\d+').cast(pl.Boolean)
) # 220231
# print(kcat)
print(kcat.shape) # (240939, 15)

cannot_find_canonical = df.filter(
    pl.col('canonical').str.contains(r'^10\.\d+_')
)
print(cannot_find_canonical.shape) # (0, 16). All accounted for!

df = df.with_columns([
    pl.col('cofactors').replace('', None).alias('cofactors')
])
df.write_parquet('data/valid/_valid_everything.parquet')

df.select(['pmid', 'canonical']).unique().write_parquet('data/pmids/everything_pmids.parquet')
