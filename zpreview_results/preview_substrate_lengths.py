import polars as pl
so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
base_df = pl.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', schema_overrides=so)
gpt_df = pl.read_csv('data/valid/_valid_beluga-t2neboth_1.csv', schema_overrides=so)

apogee_df = pl.read_parquet('data/_compiled/apogee_all.parquet')

# get the max length of the substrates
max_len = 0
max_full_len = 0

for df in [base_df, gpt_df, apogee_df]:
    
#     apogee_df['substrate'].str.len_chars().max()
# apogee_df['substrate_full'].str.len_chars().max()
    max_len = max(max_len, df['substrate'].str.len_chars().max())
    max_full_len = max(max_full_len, df['substrate_full'].str.len_chars().max())
print(max_len, max_full_len)
# 184, 185
