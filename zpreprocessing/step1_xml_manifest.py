import os
import polars as pl

from enzyextract.utils.doi_management import doi_to_filename
from enzyextract.utils.pmid_doi_convert import find_canonical

manifest = []
# fully walk
target_folders = [r'D:/papers/wosvier', 
                r'D:/papers/openelse']
for target_folder in target_folders:
    for root, dirs, files in os.walk(target_folder):
        for filename in files:
            if filename.endswith('.xml'):
                manifest.append((root.replace('\\', '/'), filename))
manifest_df = pl.DataFrame(manifest, orient='row', schema=['fileroot', 'filename'])

### convert filenames to canonical
manifest_df = manifest_df.with_columns([
    # pl.col("filename").str.replace(r'\.pdf$', '').alias("pmid"),
    (pl.col("filename").map_elements(lambda x: find_canonical(x), return_dtype=pl.Utf8)).alias("canonical"),
])
manifest_df = manifest_df.with_columns([
    (pl.col("canonical").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("canonical_filename"),
])
manifest_df = manifest_df.with_columns([
    pl.col("canonical").map_elements(lambda x: doi_to_filename(x, ''), return_dtype=pl.Utf8).alias("pmid"),
])
manifest_df = manifest_df.with_columns([
    pl.coalesce(pl.col("pmid"), 
                pl.col("filename").str.replace('\.xml$', '').str.to_lowercase())
                .alias("pmid")
])
manifest_df = manifest_df.with_columns([
    (pl.col("canonical_filename") != pl.col("filename")).alias("needs_rename"),
])

### TODO: add a column for if abstract is there

### TODO: add if processed by gpt once

### TODO: add if bucket valid

### TODO: bucket kinetic

### TODO: bucket kcat

### add toplevel/secondlevel
manifest_df = manifest_df.with_columns([
    pl.col('fileroot').str.extract_groups(r"^D:/papers/([-\w]+)/([-\w]+)").alias('namespace'),
]).unnest('namespace').rename({'1': 'toplevel', '2': 'secondlevel'})
# manifest.drop_in_place('namespace')


### add sabiork
sabiork = pl.read_parquet('data/sabiork/sabiork.parquet')
sabiork = sabiork.select('PubMedID').unique()
sabiork = sabiork.cast({'PubMedID': pl.Utf8})
manifest_df = manifest_df.with_columns([
    (pl.col('canonical').is_in(sabiork)).alias('sabiork')
])



manifest_df.write_parquet('data/xml_manifest.parquet')
