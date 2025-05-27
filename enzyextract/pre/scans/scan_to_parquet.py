"""
Read PDFs into text, for compact storage
"""

import os
from tqdm import tqdm
import pymupdf
import polars as pl

from enzyextract.dependency.prereqs import export

def scan_papers(pdfs_folder, recursive=False):
    # round up all the PDFs
    pdfs = []
    if not recursive:
        for filename in os.listdir(pdfs_folder):
            if filename.endswith('.pdf'):
                pmid = filename[:-4]
                pdfs.append((pdfs_folder, filename, pmid))
    else:
        for dirpath, dirnames, filenames in os.walk(pdfs_folder):
            for filename in filenames:
                if filename.endswith('.pdf'):
                    pmid = filename[:-4]
                    pdfs.append((dirpath, filename, pmid))
    

    # begin reading PDFs
    content = []
    for dirpath, filename, pmid in tqdm(pdfs):
        found_something = False
        try:
            pdf = pymupdf.open(os.path.join(dirpath, filename))
        except:
            continue
        for i, page in enumerate(pdf):
            text = page.get_text()
            content.append((pmid, i, text))
            
        pdf.close()

    # form df
    df = pl.DataFrame(content, schema=['pmid', 'page_number', 'text'], orient='row', schema_overrides={
        'pmid': pl.Utf8,
        'page_number': pl.UInt32,
        'text': pl.Utf8
    })
    return df


@export("data/scans/{alias}.parquet")
def script_scan_papers(pdf_folder, alias, *, recursive=True):
    df = scan_papers(pdf_folder, recursive=recursive)
    os.makedirs('data/scans', exist_ok=True)
    df.write_parquet(f'data/scans/{alias}.parquet')

if __name__ == "__main__":
    script_scan_papers("D:/papers/brenda/wiley", "brenda_wiley")