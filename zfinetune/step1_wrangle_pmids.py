import re
import polars as pl
import os
import shutil

# step 1a: determine pmids
finetune_src = 'zfinetune/mds/train manifold.md'

pmids_re = "^## PMID: (\d+)$"
pmids = []
with open(finetune_src, 'r') as f:
    for line in f:
        m = re.match(pmids_re, line)
        if m:
            pmids.append(m.group(1) + '.pdf')

# print(len(pmids)) 68
# write to parquet
pl.DataFrame(pmids, schema=['filename']).write_parquet('data/pmids/manifold_tune.parquet')
exit(0)

# step 1b: map pmid name to locations
manifest = pl.read_parquet('data/manifest.parquet').filter(
    pl.col('filename').is_in(pmids)
    & pl.col('readable')
).select(['fileroot', 'filename']).unique('filename')

# step 1c: copy them over
dest = 'C:/conjunct/tmp/eval/manifold_tune/pdfs'
os.makedirs(dest, exist_ok=True)
for row in manifest.iter_rows():
    src = row[0]
    filename = row[1]
    shutil.copy(src + '/' + filename, dest + '/' + filename)
    

print("Done")