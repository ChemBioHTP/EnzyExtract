# of use: lit_pubmed
import os
import polars as pl

from tqdm import tqdm
import time

try:
    from enzyextract.fetch_sequences.query_uniprot import fetch_uniprots_from_pmids
except ImportError:
    from query_uniprot import fetch_uniprots_from_pmids

def submit_pmid2uniprot(df: pl.DataFrame, write_to,
                            chunk_size=50,
                            ):
    """
    df: a polars dataframe with a column 'pmid' (pl.Utf8) that contains all pmids

    write_to: the path to write the parquet file to.
    
    """

    # Uniprot didn't return all 50?
    assert not os.path.exists(write_to), "Uniprot file already exists: " + write_to

    idents = df['pmid'].drop_nulls().unique().sort().to_list()
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
            fail_df = pl.DataFrame({'uniprot': batch})
            uniprot_df = pl.concat([uniprot_df, fail_df], how='diagonal')
            continue

        uniprot_df = pl.concat([uniprot_df, appendage], how='diagonal')
        
        # Continuously update the file
        uniprot_df.write_parquet(write_to)
        
        # wait for rate limit
        time.sleep(1)

if __name__ == '__main__':
    thedata = pl.read_parquet('data/export/TheData_kcat.parquet')
    relevant_pmids = thedata.select('canonical').unique().filter(
        ~pl.col('canonical').str.contains('[A-Za-z/_]')
    )
    # print(relevant_pmids)
    relevant_pmids = relevant_pmids.rename({'canonical': 'pmid'}).head(100)
    print("Have", relevant_pmids.height, "pmids")

    # in batches of 50
    ts = time.strftime("%Y%m%d-%H%M%S")
    write_to = f'data/enzymes/accessions/uniprot_from_pmid/p2u_{ts}.parquet'
    print("Writing to", write_to)
    submit_pmid2uniprot(relevant_pmids, write_to)