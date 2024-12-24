import polars as pl

so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}


df = pl.read_csv('data/valid/_valid_apogee-rebuilt.csv', schema_overrides=so)
df.write_parquet('data/valid/_valid_apogee-rebuilt.parquet')