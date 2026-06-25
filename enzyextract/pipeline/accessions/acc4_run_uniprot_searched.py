import os
import polars as pl
import time

from tqdm import tqdm

from enzyextract.dependency.injection import REQUIRE, resolve


from enzyextract.dependency.prereqs import export
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.enzyme_io import read_all_dfs

from enzyextract.fetch_sequences.query_uniprot import get_batch, extract_uniprotkb_fields
from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress
from enzyextract.thesaurus.organism_patterns import pl_fix_organism


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
                
                # NOTE: typo bug previously logged errors under the wrong columns
                # ('organism' and 'enzyme_preferred')
                # which necessitated a post processing fix "_fix_search_blunder". 
                # This has been fixed, so post processing is no longer needed. 
                fail_df = pl.DataFrame({'query_organism': [query_organism], 'query_enzyme': [query_enzyme]})
                uniprot_df = pl.concat([uniprot_df, fail_df], how='diagonal_relaxed')
                continue

            uniprot_df = pl.concat([uniprot_df, appendage], how='diagonal_relaxed')
            
            # Continuously 
            
            # wait for rate limit
            time.sleep(1)
        # del uniprot_df # save memory ???
        # update the file
        uniprot_df.write_parquet(write_to)
        uniprot_df = uniprot_df.clear()

@resolve
def main(
    thedata = REQUIRE('data/export/TheData_bare.parquet')
):
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

    already_done = read_all_dfs('data/enzymes/accessions/uniprot_searched')
    if already_done is not None:
        thedata = thedata.join(already_done, left_on=['organism', 'enzyme_preferred'], 
                            right_on=['query_organism', 'query_enzyme'], how='anti')
    # exclude already done

    # print(relevant_pmids)
    print("Have", thedata.height, "items")
    data_view = thedata
    
    # in batches of 50
    write_to_folder = 'data/enzymes/accessions/uniprot_searched'
    print("Writing to", write_to_folder)
    submit_names2uniprot(data_view, write_to_folder)

def _fix_search_blunder(df):
    """
    Previous typo meant that when Uniprot search failed,
    the query_organism and query_enzyme were logged under the wrong columns
    (organism and enzyme_preferred). This function fixes that.
    """

    df_normal = df.filter(
        pl.col('query_organism').is_not_null()
    )
    df_exception = df.filter(
        pl.col('query_organism').is_null()
    )
    df_exception = df_exception.with_columns(
        pl.col('organism').alias('query_organism'),
        pl.col('enzyme_preferred').alias('query_enzyme'),
        pl.lit(None).alias('organism'),
        pl.lit(None).alias('enzyme_preferred'),
    )
    df = pl.concat([df_normal, df_exception], how='diagonal').drop('enzyme_preferred')
    return df

@export("data/thesaurus/enzymes/uniprots_searched.parquet")
def generate_searched_chapter():

    df = read_all_dfs('data/enzymes/accessions/uniprot_searched')

    df = df.with_columns(
        pl_fix_organism(pl.col('query_organism')).alias('query_organism_fixed'),
    )

    # calculate similarity between query_enzyme and protein_name
    # similarities = df.select(['query_enzyme', 'enzyme_name']).unique()
    # def similarity(x):
    #     query_enzyme = x['query_enzyme']
    #     protein_name = x['enzyme_name']
    #     out = rapidfuzz.fuzz.partial_ratio(query_enzyme, protein_name) # 0 to 100
    #     return out
    
    comparisons = [
        # ('enzyme_preferred', 'enzyme_name', False, 'similarity_enzyme_name'),
        ('query_organism_fixed', 'organism', False, 'similarity_organism'),
        ('query_organism', 'organism_common', False, 'similarity_organism_common'),
        ('query_enzyme', 'recommended_name', False, 'similarity_recommended_name'),
        ('query_enzyme', 'submission_names', False, 'similarity_submission_names'),
        ('query_enzyme', 'alternative_names', False, 'similarity_alternative_names'),
    ]

    df = compute_fuzz_with_progress(df, comparisons).with_columns(
        pl.max_horizontal(
            pl.col("similarity_organism"),
            pl.col("similarity_organism_common"),
        ).alias('max_organism_similarity'),
        pl.max_horizontal(
            pl.col("similarity_recommended_name"),
            pl.col("similarity_submission_names"),
            pl.col("similarity_alternative_names"),
        ).alias('max_enzyme_similarity'),
    )

    acceptable = df.filter(
        (pl.col('max_organism_similarity') >= 80) & (pl.col('max_enzyme_similarity') >= 60)
    )
    acceptable = acceptable.with_columns([
        (pl.col('max_organism_similarity').fill_null(50) + pl.col('max_enzyme_similarity').fill_null(0)).alias('total_similarity')
    ]).sort('total_similarity', descending=True).unique(['query_organism', 'query_enzyme'], keep='first')
    return acceptable

if __name__ == '__main__':
    main()