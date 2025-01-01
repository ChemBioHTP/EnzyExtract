"""
How useful are abbreviations?
"""

import polars as pl

abbr_df  = pl.read_parquet('data/thesaurus/abbr/beluga_abbrs.parquet')

so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}
base_df = pl.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', schema_overrides=so)

# join enzyme to a_name
# join enzyme_full to b_name

joined_df = base_df.join(abbr_df, left_on=['pmid', 'enzyme'], right_on=['pmid', 'a_name'], how='left')
print(joined_df)

# compare and contrast GPT abbreviation parsing and automatic abbreviation parsing



gpt_df = pl.read_parquet('data/thesaurus/abbr/beluga-abbrs-4ostruct_20241213.parquet')
gpt_df = gpt_df[['abbreviation', 'full_name', 'category', 'pmid']]

print("GPT has max full_name length of", gpt_df['full_name'].str.len_chars().max())

interesting_df = abbr_df.join(gpt_df, left_on=['pmid', 'a_name'], right_on=['pmid', 'abbreviation'], how='full')
interesting_df = interesting_df.with_columns([
    (pl.col('b_name').str.replace_all('\s+', ' ').str.strip_chars() == pl.col('full_name').str.replace_all('\s+', ' ').str.strip_chars()).alias('same_name'),
])
print(interesting_df) # 86% the same