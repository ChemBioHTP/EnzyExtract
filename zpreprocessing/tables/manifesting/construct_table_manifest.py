# construct manifest file of table 
import os
import polars as pl

collector = []

for root, dirs, files in os.walk('C:/conjunct/vandy/yang/corpora/tabular'):
    for filename in files:
        if filename.endswith('.info'):
            collector.append((root.replace('\\', '/'), filename))

df = pl.DataFrame(collector, orient='row', schema=['fileroot', 'filename'])

df = df.with_columns(
    pl.col('fileroot').str.extract_groups(r'C:/conjunct/vandy/yang/corpora/tabular/([^/]+)/?(.*)$').alias('levels'),
    pl.col('filename').str.extract(r'(.*)_\d+\.(rotated\.?)?info$').alias('pmid')
)


df = df.unnest('levels').rename({"1": "toplevel", "2": "secondlevel"})
df.write_parquet('zpreprocessing/data/table_manifest_info.parquet')