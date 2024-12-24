import re
import polars as pl
import os
import shutil

def wrangle_remote_missed_pmids():
    manifest = pl.read_parquet('data/manifest.parquet').filter(
        pl.col('fileroot').str.contains('wos/remote_all')
    )

    df = pl.read_parquet('data/pmids/remote_manifest.parquet')

    # get those not in df
    missing = manifest.filter(
        ~pl.col('filename').is_in(df['filename'])
    )
    print(missing)

    
    # step 1b: map pmid name to locations
    # shuttle = missing.select(['fileroot', 'filename']).unique('filename')

    # # step 1c: copy them over
    # dest = 'C:/conjunct/tmp/remote_all_shuttle/pdfs'
    # os.makedirs(dest, exist_ok=True)
    # for row in shuttle.iter_rows():
    #     src = row[0]
    #     filename = row[1]
    #     shutil.copy(src + '/' + filename, dest + '/' + filename)
    
def wrangle_scratch_remote_missed_pmids():
    # handle those in C:\conjunct\vandy\yang\corpora\scratch\remote

    pdfs = set()
    # for pdf in os.listdir('C:/conjunct/vandy/yang/corpora/scratch/remote'):

    # those are the PDFs that had the keyword "km", it was pretty useless in turning up kinetics papers.
    # for pdf in os.listdir('C:/conjunct/vandy/yang/corpora/screen/subset'):
    for pdf in os.listdir('C:/conjunct/vandy/yang/corpora/wos/remote/hybrid'):
        if pdf.endswith('.pdf'):
            pdfs.add(pdf)
    
    manifest = pl.read_parquet('data/manifest.parquet').filter(
        # pl.col('fileroot').str.contains('scratch') &
        pl.col('filename').is_in(pdfs)
    ).unique('filename')
    print(manifest) 
    
    # scratch/remote:
    # 837/837. Oh thank god, we do have them all.
    # they are all in scratch/open

    # wos/remote/hybrid:
    # 450/450. Oh thank god, we do have them all.
    # they are all in wos/remote_all

if __name__ == '__main__':
    # wrangle_remote_missed_pmids()
    # print("Done")

    wrangle_scratch_remote_missed_pmids()