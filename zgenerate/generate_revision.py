import polars as pl


df = pl.read_parquet('data/gpt/revision-prod_gpt.parquet')

### select the desired split
df = df.filter(
    pl.col('custom_id').str.starts_with('tablevision-prod1_')
).select(['pmid', 'content'])
# content is json, so we need to parse it
df = df.with_columns(
    pl.col('content').str.json_decode().alias('json')
).unnest('json')

df = df.with_columns(
    (pl.col('kcat_exponents').is_null()
    | (pl.col('kcat_exponents').list.len() == 0)
    | pl.col('kcat_exponents').list.eval(pl.element().is_in(["10^0", "0"])).list.all()
    ).alias('no_kcat_scientific_notation'),
)
print(df)
