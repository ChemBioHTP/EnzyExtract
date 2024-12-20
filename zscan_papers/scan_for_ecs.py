import polars as pl
import os
import pymupdf
import re
from tqdm import tqdm

def script_look_for_ecs(pdfs_folder="C:/conjunct/tmp/eval/arctic", recursive=False):
    # look for EC numbers in the PDFs

    ec_re = re.compile(r'EC\s*(\d+\.\d+\.\d+\.\d+)')

    
    ec_matches = []
    # for filename in tqdm(os.listdir(pdfs_folder)):
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
    for dirpath, filename, pmid in tqdm(pdfs):
        found_something = False


        try:
            pdf = pymupdf.open(os.path.join(dirpath, filename))
        except:
            continue
        for page in pdf:
            text = page.get_text()

            matches = ec_re.findall(text)
            for match in matches:
                ec_matches.append((pmid, match))
                found_something = True
        if not found_something:
            ec_matches.append((pmid, None))
            
        pdf.close()

    df = pl.DataFrame(ec_matches, schema=['pmid', 'ec'], orient='row', schema_overrides={
        'pmid': pl.Utf8,
        'ec': pl.Utf8
    })#  columns=['pmid', 'ec'])
    # df.to_csv('C:/conjunct/tmp/eval/arctic_dev/ec_matches.csv', index=False)
    return df

def script_look_for_all_ecs():
    # dfs = []
    df = script_look_for_ecs("D:/papers/brenda", recursive=True)
    df.write_parquet('data/enzymes/ec/brenda_ec_matches.parquet')
    df = script_look_for_ecs("D:/papers/scratch", recursive=True)
    df.write_parquet('data/enzymes/ec/scratch_ec_matches.parquet')
    df = script_look_for_ecs("D:/papers/topoff", recursive=True)
    df.write_parquet('data/enzymes/ec/topoff_ec_matches.parquet')
    df = script_look_for_ecs("D:/papers/wos", recursive=True)
    df.write_parquet('data/enzymes/ec/wos_ec_matches.parquet')
    
    # df = pl.concat(dfs)
    # df.to_csv('data/enzymes/ec/pdf_ec_matches.csv', index=False)
    # df.write_parquet('data/enzymes/ec/pdf_ec_matches.parquet')


def script_ec_success_rate():

    pmid2ec = pl.read_parquet('data/enzymes/ec/ec_matches.parquet')
    # filter to only capture \d+\.\d+\.\d+\.\d+
    pmid2ec = pmid2ec.with_columns(
        pl.col('ec').str.extract(r'(\d+\.\d+\.\d+\.\d+)', group_index=1).alias('ec')
    ).unique().drop_nulls(subset=['ec'])

    pmid2ec = pmid2ec.group_by('pmid').agg('ec').rename({'ec': 'ecs_from_pmid'})
    mydata = pl.read_csv('data/humaneval/runeem/runeem_20241205_ec.csv', schema_overrides={
        'pmid': pl.Utf8
    })
    mydata = mydata.with_columns(
        pl.col('viable_ecs').str.split('; ').alias('viable_ecs')
    )
    # do a join
    mydata = mydata.join(pmid2ec, on='pmid', how='left')

    mydata = mydata.with_columns([
        (pl.col('viable_ecs').is_not_null() | pl.col('ecs_from_pmid').is_not_null()).alias('has_ecs'),
        ((pl.col('viable_ecs').list.set_intersection(pl.col('ecs_from_pmid')))
            .list.len() > 0).alias('has_common_ecs'),
    ])

    mydata.write_parquet('data/enzymes/ec/runeem_20241205_ec_counts.parquet')

    print(mydata)

if __name__ == '__main__':
    # df = script_look_for_ecs()
    script_look_for_all_ecs()
    # script_ec_success_rate()
    pass