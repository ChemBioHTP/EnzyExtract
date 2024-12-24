import os
import polars as pl

from enzyextract.utils.doi_management import doi_to_filename
from enzyextract.utils.pmid_doi_convert import find_canonical

manifest = []
# fully walk
target_folders = [r'D:/papers/brenda', 
                r'D:/papers/wos', 
                r'D:/papers/topoff', 
                r'D:/papers/scratch']
for target_folder in target_folders:
    for root, dirs, files in os.walk(target_folder):
        for filename in files:
            if filename.endswith('.pdf'):
                manifest.append((root.replace('\\', '/'), filename))
manifest_df = pl.DataFrame(manifest, orient='row', schema=['filepath', 'filename'])

# convert filenames to canonical
manifest_df = manifest_df.with_columns([
    (pl.col("filename").map_elements(lambda x: find_canonical(x), return_dtype=pl.Utf8)).alias("canonical"),
])

manifest_df = manifest_df.with_columns([
    (pl.col("canonical").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("canonical_filename"),
])
manifest_df = manifest_df.with_columns([
    (pl.col("canonical_filename") != pl.col("filename")).alias("needs_rename"),
])

# add a column for readable by pymupdf
readable_dfs = []
for filename in ['brenda', 'scratch', 'topoff', 'wos']:
    df = pl.scan_parquet(f'data/scans/{filename}.parquet').select('pmid').unique().collect()
    df = df.with_columns([
        (pl.col("pmid") + '.pdf').alias("filename"),
    ])
    readable_dfs.append(df)

readable_df = pl.concat(readable_dfs)
readable_filenames = readable_df.select("filename").unique()

manifest_df = manifest_df.with_columns([
    pl.col("filename").is_in(readable_filenames).alias("readable"),
])

# add a column for if apogee finds any kinetic value
kinetic_df = pl.read_parquet('data/pmids/apogee_nonbrenda_numerical.fn.parquet')
kinetic_df = kinetic_df.with_columns([
    # (pl.col("pmid") + '.pdf').alias("filename"),
    (pl.col("pmid").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("filename"),
])
kinetic_filenames = kinetic_df.select("filename").unique()

manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(kinetic_filenames) | pl.col("canonical_filename").is_in(kinetic_filenames)
    #  | pl.col("filepath").str.starts_with("D:/papers/brenda")
    ).alias("kinetic"),
])


# add if processed by gpt once
gpt_df = pl.scan_parquet('data/gpt/apogee_gpt.parquet')
gpt_filenames = gpt_df.select(
    # (pl.col("pmid") + '.pdf').alias("filename"),
    (pl.col("pmid").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("filename"),
).unique().collect()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(gpt_filenames) | pl.col("canonical_filename").is_in(gpt_filenames)
    ).alias("apogee_processed"),
])

# add if valid
so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}

valid_df = pl.scan_csv('data/_compiled/apogee-nonbrenda.tsv', separator='\t', schema_overrides=so)
valid_filenames = valid_df.select(
    (pl.col("pmid").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("filename"),
).unique().collect()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(valid_filenames) | pl.col("canonical_filename").is_in(valid_filenames)
    ).alias("apogee_valid"),
])


manifest_df = manifest_df.with_columns([
    pl.col('filepath').str.extract_groups(r"^D:/papers/([-\w]+)/([-\w]+)").alias('namespace'),
]).unnest('namespace').rename({'1': 'toplevel', '2': 'secondlevel'})
# manifest.drop_in_place('namespace')

manifest_df.write_parquet('zpreprocessing/data/manifest.parquet')
