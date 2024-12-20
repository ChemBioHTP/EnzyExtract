# consider preprocessing data to be separate
import os
import polars as pl
import json
from tqdm import tqdm

target_folders = [r'C:/conjunct/vandy/yang/corpora/tabular/brenda', 
                  r'C:/conjunct/vandy/yang/corpora/tabular/wos', 
                  r'C:/conjunct/vandy/yang/corpora/tabular/topoff', 
                  r'C:/conjunct/vandy/yang/corpora/tabular/scratch']
# recursive
pdfs = []
for target_folder in target_folders:
    for root, dirs, files in os.walk(target_folder):
        for filename in files:
            if not filename.endswith('.info'):
                continue
            pdfs.append((root, filename))

collector = []
for root, filename in tqdm(pdfs):
    with open(f"{root}/{filename}", 'r') as f:
        info = json.load(f)
    collector.append(info)
df = pl.DataFrame(collector)
print(df)
df.write_parquet('zpreprocessing/data/pdf_tables.parquet')