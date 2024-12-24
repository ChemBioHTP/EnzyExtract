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
manifest_df = pl.DataFrame(manifest, orient='row', schema=['fileroot', 'filename'])

### convert filenames to canonical
manifest_df = manifest_df.with_columns([
    pl.col("filename").str.replace(r'\.pdf$', '').alias("pmid"),
    (pl.col("filename").map_elements(lambda x: find_canonical(x), return_dtype=pl.Utf8)).alias("canonical"),
])

manifest_df = manifest_df.with_columns([
    (pl.col("canonical").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("canonical_filename"),
])
manifest_df = manifest_df.with_columns([
    (pl.col("canonical_filename") != pl.col("filename")).alias("needs_rename"),
])

### add a column for readable by pymupdf
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




### add if processed by gpt once
gpt_df = pl.scan_parquet('data/gpt/apogee_gpt.parquet')
gpt_filenames = gpt_df.select(
    # (pl.col("pmid") + '.pdf').alias("filename"),
    (pl.col("pmid").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("filename"),
).unique().collect()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(gpt_filenames) | pl.col("canonical_filename").is_in(gpt_filenames)
    ).alias("apogee_processed"),
])

### add if valid
so = {'pmid': pl.Utf8, 'km_2': pl.Utf8, 'kcat_2': pl.Utf8, 'kcat_km_2': pl.Utf8, 'pH': pl.Utf8, 'temperature': pl.Utf8}

# valid_df = pl.scan_csv('data/_compiled/apogee-nonbrenda.tsv', separator='\t', schema_overrides=so)
# valid_df = pl.read_csv('data/valid/_valid_apogee-rebuilt.csv', schema_overrides=so)
valid_df = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
valid_df = valid_df.with_columns([
    (pl.col("pmid").map_elements(lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8)).alias("filename"),
])
valid_filenames = valid_df.select(pl.col("filename")).unique()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(valid_filenames) | pl.col("canonical_filename").is_in(valid_filenames)
    ).alias("apogee_valid"),
])


### add a column for if apogee finds any kinetic value
kinetic_df = valid_df.filter(
    (pl.col('kcat').str.contains(r'\d+').cast(pl.Boolean)) | 
    (pl.col('km').str.contains(r'\d+').cast(pl.Boolean))
)
kinetic_filenames = kinetic_df.select("filename").unique()

manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(kinetic_filenames) | pl.col("canonical_filename").is_in(kinetic_filenames)
    #  | pl.col("fileroot").str.starts_with("D:/papers/brenda")
    # ).alias("kinetic"),
    ).alias("apogee_kinetic"),
])

### add kcat
kcat_df = valid_df.filter(
    pl.col('kcat').str.contains(r'\d+').cast(pl.Boolean)
)
kcat_filenames = kcat_df.select("filename").unique()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(kcat_filenames) | pl.col("canonical_filename").is_in(kcat_filenames)
    ).alias("apogee_kcat"),
])

### add toplevel/secondlevel
manifest_df = manifest_df.with_columns([
    pl.col('fileroot').str.extract_groups(r"^D:/papers/([-\w]+)/([-\w]+)").alias('namespace'),
]).unnest('namespace').rename({'1': 'toplevel', '2': 'secondlevel'})
# manifest.drop_in_place('namespace')


### add if had gmft
tables_df = pl.scan_parquet('zpreprocessing/data/pdf_tables.parquet').select("filename").unique().collect()

table_filenames = tables_df.select("filename").unique()
manifest_df = manifest_df.with_columns([
    (pl.col("filename").is_in(table_filenames) | pl.col("canonical_filename").is_in(table_filenames))
    .alias("had_gmft"),
])

### add if bad ocr
bad_ocr = pl.read_parquet('data/scans/ocr/bad_ocr.parquet').with_columns([
    (pl.col('pmid') + '.pdf').alias('filename'),
    pl.lit(True).alias('bad_ocr'),
]).select(['filename', 'toplevel', 'bad_ocr'])
manifest_df = manifest_df.join(bad_ocr, on=['filename', 'toplevel'], how='left').with_columns(
    (pl.col('bad_ocr').is_not_null()).alias('bad_ocr')
)

### add sabiork
sabiork = pl.read_parquet('data/sabiork/sabiork.parquet')
sabiork = sabiork.select('PubMedID').unique()
sabiork = sabiork.cast({'PubMedID': pl.Utf8})
manifest_df = manifest_df.with_columns([
    (pl.col('canonical').is_in(sabiork)).alias('sabiork')
])



manifest_df.write_parquet('data/manifest.parquet')
