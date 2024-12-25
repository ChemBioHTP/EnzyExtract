import polars as pl
from tqdm import tqdm
manifest = pl.read_parquet('data/xml_manifest.parquet')

manifest = manifest.select(['fileroot', 'filename'])

content = []
for fileroot, filename in tqdm(manifest.iter_rows()):
    filepath = f"{fileroot}/{filename}"
    with open(filepath, "r", encoding='utf-8') as f:
        content.append(f.read())

manifest = manifest.with_columns(pl.Series("content", content))