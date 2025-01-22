import os
import polars as pl
import requests
import time

from tqdm import tqdm


try:
    from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
    from enzyextract.thesaurus.enzyme_io import read_all_dfs

    from enzyextract.fetch_sequences.query_uniprot import get_batch, extract_uniprotkb_fields
except ImportError:
    from ascii_patterns import pl_to_ascii
    from enzyme_io import read_all_dfs
    from query_uniprot import get_batch, extract_uniprotkb_fields


def _fetch_uniprot_search_latest(query_organism, query_enzyme):
    """
    note that there can be a lot more fields to fetch, so only do one at a time
    """
    # Use UniProt API to fetch information in bulk
    url = "https://rest.uniprot.org/uniprotkb/search"
    # query = " OR ".join(f'(accession:{x})' for x in uniprot_ids)
    query = f"organism_name:{query_organism} AND {query_enzyme}"
    params = {
        "query": query,
        # "fields": "accession,id,protein_name,organism_name,sequence,ec", # lit_pubmed_id
        "format": "json",
        "size": 50 # we don't need that many
    } # https://www.uniprot.org/help/return_fields
    # response = requests.get(url, params=params)
    # return response.json()
    agg = {}
    for response, total in get_batch(url, params):
        data = response.json()
        if "results" not in agg:
            agg["results"] = data.get("results", [])
        else:
            agg["results"] += data.get("results", [])
        agg["total"] = total
    return agg

def fetch_names2uniprot(query_organism, query_enzyme) -> list:
    # https://rest.uniprot.org/uniprotkb/search?query=organism_name:Escherichia+coli+AND+superoxide+dismutase&fields=accession,protein_name,organism_name,sequence
    uniprot_info = _fetch_uniprot_search_latest(query_organism, query_enzyme)
    
    # get all accessions
    results = {
        'query_uniprot': [],
        'query_organism': [],
        'query_enzyme': [],
        'uniprot': [],
        'uniprot_aliases': [],
        'enzyme_name': [],
        'organism': [],
        'organism_common': [],
        'sequence': [],
        'ec_numbers': [],
        'pmids': [],
        'dois': [],
        'uniparc': [],
        'why_deleted': [],
        'full_response': [],
    }
    for entry in uniprot_info['results']:
        extract_uniprotkb_fields(entry, results, want_uniprot=None) 
        # want_uniprot set query_uniprot to None
        results['query_enzyme'].append(query_enzyme)
        results['query_organism'].append(query_organism)
    
        
    df = pl.DataFrame(results, schema_overrides={
        'query_uniprot': pl.Utf8,
        'query_organism': pl.Utf8,
        'query_enzyme': pl.Utf8,
        'uniprot': pl.Utf8,
        'uniprot_aliases': pl.List(pl.Utf8),
        'enzyme_name': pl.Utf8,
        'organism': pl.Utf8,
        'organism_common': pl.Utf8,
        'sequence': pl.Utf8,
        'ec_numbers': pl.List(pl.Utf8),
        'dois': pl.List(pl.Utf8),
        'pmids': pl.List(pl.Utf8),
        'uniparc': pl.Utf8,
        'why_deleted': pl.Utf8,
        'full_response': pl.Utf8,
    })
    return df       

def submit_names2uniprot(df: pl.DataFrame, write_to_folder,
                            batch_size=100,
                            ):
    """
    df: a polars dataframe with columns 'organism' (pl.Utf8) and 'enzyme_preferred' (pl.Utf8)

    write_to: the path to write the parquet file to.
    Produces: the normal df produced by extract_uniprotkb_fields, PLUS 2 extra columns: 'query_organism' and 'query_enzyme'
    
    """

    # Uniprot didn't return all 50?
    # assert not os.path.exists(write_to_folder), "Uniprot file already exists: " + write_to

    # idents = df['pmid'].drop_nulls().unique().sort().to_list()
    uniprot_df = pl.DataFrame()
    for sliced in df.iter_slices(batch_size):
        ts = time.strftime("%Y%m%d-%H%M%S")
        write_to = f'{write_to_folder}/n2u_{ts}.parquet'
        if os.path.exists(write_to):
            raise ValueError("Already exists")

        for query_organism, query_enzyme in tqdm(sliced.select(['organism', 'enzyme_preferred']).iter_rows(), total=batch_size):
            try:
                appendage = fetch_names2uniprot(query_organism, query_enzyme)
            except Exception as e:
                print("Error fetching", query_enzyme, query_organism)
                print(e)
                # if uniprot_df.height == 0:
                    # raise e # if no data has been fetched, raise the error
                fail_df = pl.DataFrame({'organism': [query_organism], 'enzyme_preferred': [query_enzyme]})
                uniprot_df = pl.concat([uniprot_df, fail_df], how='diagonal')
                continue

            uniprot_df = pl.concat([uniprot_df, appendage], how='diagonal')
            
            # Continuously 
            
            # wait for rate limit
            time.sleep(1)
        # del uniprot_df # save memory ???
        # update the file
        uniprot_df.write_parquet(write_to)
        uniprot_df = uniprot_df.clear()

if __name__ == '__main__':
    
    thedata = pl.read_parquet('data/export/TheData_kcat.parquet')

    thedata = thedata.with_columns(
        pl_to_ascii(
            pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme')),
            lowercase=False
        ).alias('enzyme_preferred'),
    ).filter(
        pl.col('enzyme_preferred').is_not_null()
        # cannot have non-ascii, or else API won't work anyways
        & (pl.col('enzyme_preferred').str.replace_all(r"[\p{Ascii}]", "").str.len_chars() == 0)
        & pl.col('organism').is_not_null()
    ).with_columns([
        # of these characters: 
        #  0123456789ABCDEF
        #2  !"#$%&'()*+,-./
        #3           :;<=>?
        #4 @
        #5            [\]^_
        #6 `{|}~
        # allowed: #$%&'*+,-./;<=>?@_`{}~
        # disallowed: !"():[\]^
        pl.col('enzyme_preferred').str.replace_all(r"[!\"():\[\]^]", "").alias('enzyme_preferred'),
        pl.col('organism').str.replace_all(r"[!\"():\[\]^]", "").alias('organism'),
    ]).select([
        'organism',
        'enzyme_preferred',
    ]).unique()

    already_done = read_all_dfs('data/enzymes/accessions/uniprot_searched/gen1')
    thedata = thedata.join(already_done, left_on=['organism', 'enzyme_preferred'], 
                           right_on=['query_organism', 'query_enzyme'], how='anti')
    # exclude already done

    already_done2 = read_all_dfs('data/enzymes/accessions/uniprot_searched/gen2')
    thedata = thedata.join(already_done2, left_on=['organism', 'enzyme_preferred'], 
                           right_on=['query_organism', 'query_enzyme'], how='anti')

    # print(relevant_pmids)
    print("Have", thedata.height, "items")
    # data_view = thedata.head(10000)
    data_view = thedata
    

    # in batches of 50
    # ts = time.strftime("%Y%m%d-%H%M%S")
    write_to_folder = f'data/enzymes/accessions/uniprot_searched/gen2'
    print("Writing to", write_to_folder)
    submit_names2uniprot(data_view, write_to_folder)

    