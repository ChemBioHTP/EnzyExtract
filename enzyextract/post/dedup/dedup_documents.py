# deduplicate 

import polars as pl

# preference: rekcat, apogee, apatch, bucket (openelse)
# see: enzy_runner/zgenerate/generate_uber_valid.py which creates:
# _valid_everything.parquet
# preference is: bucket, apogee, apatch
# though documents should have already been deduplicated



data_df = pl.read_parquet('data/recontext/1_fromyaml/data.parquet')
context_df = pl.read_parquet('data/recontext/1_fromyaml/context.parquet')

bad_custom_ids = context_df.filter(
    context_df.select('custom_id').is_duplicated()
).sort('custom_id')
print(bad_custom_ids) # 564
# REASON: GPT provides two yamls: one, normally; the second one, the "final answer" (the exact same one).

doc_scanned_twice = context_df.unique(['pmid', 'custom_id'], keep='first')
doc_scanned_twice = doc_scanned_twice.filter(
    doc_scanned_twice.select('pmid').is_duplicated()
).sort('pmid')
print(doc_scanned_twice) # 5706
pass #

