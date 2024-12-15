"""
Read PDFs into text, and see how compact they are.
"""

import os
from tqdm import tqdm
import pymupdf
import polars as pl

def scan_papers(pdfs_folder, recursive=False):
    # look for EC numbers in the PDFs
    

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

def script_look_for_all_ecs():
    df = scan_papers("D:/papers/brenda", recursive=True)
    df.write_parquet("data/scans/brenda.parquet")
    # print(df)
    # return df

if __name__ == '__main__':
    script_look_for_all_ecs()