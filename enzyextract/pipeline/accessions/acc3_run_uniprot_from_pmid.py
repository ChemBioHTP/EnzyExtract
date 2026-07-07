# of use: lit_pubmed
import os
from pathlib import Path
from typing import Union
import polars as pl

from tqdm import tqdm
import time

from enzyextract.dependency.injection import REQUIRE, resolve

try:
    from enzyextract.fetch_sequences.query_uniprot import fetch_uniprots_from_pmids
except ImportError:
    from query_uniprot import fetch_uniprots_from_pmids


def pmid2uniprot_batch_downloader(
    df: pl.DataFrame,
    write_folder: Union[str, Path],
    chunk_size=50,
):
    """
    df: a polars dataframe with a column "pmid" (pl.Utf8) that contains all pmids

    write_folder: the folder to write the parquet file to.
    
    """

    ts = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    write_to = f"{write_folder}/pmid_to_uniprot/pmid_to_uniprot_{ts}.parquet"

    # Uniprot didn"t return all 50?
    assert not os.path.exists(write_to), "Uniprot file already exists: " + write_to

    Path(write_folder).mkdir(parents=True, exist_ok=True)

    idents = df["pmid"].drop_nulls().unique().sort().to_list()
    uniprot_df = pl.DataFrame()
    for i in tqdm(range(0, len(idents), chunk_size)):
        batch = idents[i:i+chunk_size]
        try:
            appendage = fetch_uniprots_from_pmids(batch)
        except Exception as e:
            print("Error fetching", batch)
            print(e)
            if uniprot_df.height == 0:
                raise e # if no data has been fetched, raise the error
            fail_df = pl.DataFrame({"uniprot": batch})
            uniprot_df = pl.concat([uniprot_df, fail_df], how="diagonal")
            continue

        uniprot_df = pl.concat([uniprot_df, appendage], how="diagonal")
        
        # Continuously update the file
        uniprot_df.write_parquet(write_to)
        
        # wait for rate limit
        time.sleep(1)

@resolve
def _main(
    thedata = REQUIRE('data/export/TheData_bare.parquet')
):
    relevant_pmids = thedata.select('canonical').unique().filter(
        ~pl.col('canonical').str.contains('[A-Za-z/_]')
    ) # get every canonical that is strictly numeric (ie. a pmid)
    # print(relevant_pmids)
    relevant_pmids = relevant_pmids.rename({'canonical': 'pmid'}).head(100)
    print("Have", relevant_pmids.height, "pmids")

    # in batches of 50
    ts = time.strftime("%Y%m%d-%H%M%S")
    write_to = f'data/enzymes/accessions/uniprot_from_pmid/p2u_{ts}.parquet'
    print("Writing to", write_to)
    pmid2uniprot_batch_downloader(relevant_pmids, write_to)

if __name__ == '__main__':
    _main()