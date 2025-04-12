"""Frown: papers that have no SMILES string."""

import polars as pl
import os
import shutil

from tqdm import tqdm

df = pl.read_parquet('C:/conjunct/EnzyExtract/pkg/data/export/subsets/TheData_kcat_smiles_recovery.parquet')
target_pmids = set(df['pmid'])
# print(list(pmids)[:10])

hoist_to = 'D:/papers/museum/frown'

walkable = [
    'brenda', 
    'scratch',
    'topoff',
    'wos',
    'openelse'
]

suitable_pdfs = []
suitable_xmls = []
found_pmids = set()
for w in walkable:
    walkable_path = f'D:/papers/{w}'
    for root, dirs, files in os.walk(walkable_path):
        for file in files:
            pmid = file.rsplit('.', 1)[0]
            found_pmids.add(pmid)
            if pmid in target_pmids:
                if file.endswith('.pdf'):
                    suitable_pdfs.append(os.path.join(root, file))
                elif file.endswith('.xml'):
                    suitable_xmls.append(os.path.join(root, file))
                

print("Started with", len(target_pmids), "files")
print("Tracked", len(suitable_pdfs), "pdfs")
print("Tracked", len(suitable_xmls), "xmls")
print("Found", len(found_pmids & target_pmids), "files")

yn = input(f"Do you want to copy the pdfs to {hoist_to}? (y/n)")
if yn.lower() == 'y':
    os.makedirs(hoist_to, exist_ok=True)
    for fpath in tqdm(suitable_pdfs):
        # COPY!!!!!
        shutil.copy(fpath, hoist_to)
        
