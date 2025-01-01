import polars as pl
import requests
import time

from tqdm import tqdm


import re
from requests.adapters import HTTPAdapter, Retry

re_next_link = re.compile(r'<(.+)>; rel="next"')
retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))

def get_next_link(headers):
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)

def get_batch(batch_url, params):
    while batch_url:
        response = session.get(batch_url, params=params)
        response.raise_for_status()
        total = response.headers["x-total-results"]
        yield response, total
        batch_url = get_next_link(response.headers)

def _fetch_uniprot_info_latest(query_organism, query_enzyme):
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
        "size": 500
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

def get_uniprot_accession(query_organism, query_enzyme) -> list:
    # https://rest.uniprot.org/uniprotkb/search?query=organism_name:Escherichia+coli+AND+superoxide+dismutase&fields=accession,protein_name,organism_name,sequence
    returning = []
    data = _fetch_uniprot_info_latest(query_organism, query_enzyme)
    
    # get all accessions
    for result in data.get('results', []):
        acc = result.get('primaryAccession', None)
        org = None
        if 'organism' in result:
            org = result['organism'].get('scientificName')
            if not org:
                org = result['organism'].get('commonName')
            if not org:
                org = (result['organism'].get('lineage') or [None])[-1]
            if not org:
                org = (result['organism'].get('synonyms') or [None])[0]
        name = None
        if 'proteinDescription' in result:
            if 'recommendedName' in result['proteinDescription']:
                # if 'fullName' in result['proteinDescription']['recommendedName']:
                name = result['proteinDescription']['recommendedName'].get('fullName', {}).get('value')
            if not name:
                if 'alternativeName' in result['proteinDescription']:
                    name = result['proteinDescription']['alternativeName'][0].get('fullName', {}).get('value')
        # sequence too
        # if 'sequence' in result:
        seq = result.get('sequence', {}).get('value', None)
        returning.append((query_organism, query_enzyme, acc, org, name, seq))
    return returning        

def process_dataframe(df):
    results = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        organism_name = row['organism_name']
        enzyme_name = row['enzyme_name']
        
        data = get_uniprot_accession(organism_name, enzyme_name)
        
        results.extend(data)
        
        # Add a small delay to avoid overwhelming the API
        time.sleep(1)
    
    return pd.DataFrame(results, columns=['query_organism', 'query_enzyme', 'accession', 'organism', 'protein_name', 'sequence'])

def create_tried():
    df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')
    df = df[['enzyme_full', 'organism']]
    df.columns = ['enzyme_name', 'organism_name']

     # get unique rows
    df = df.drop_duplicates()
    
    # require both organism and enzyme name
    df = df.dropna()

    content = []
    content.append(df.copy())

    df = pd.read_csv('data/_compiled/apogee-all.tsv', sep='\t')
    # perform the rename enzyme_full --> enzyme_name, and organism --> organism_name

    # enzyme_preferred: enzyme_full, fill na with enzyme, then enzyme_2
    df['enzyme_preferred'] = df['enzyme_full'].fillna(df['enzyme']).fillna(df['enzyme_2'])
    df = df[['enzyme_preferred', 'organism']]
    df.columns = ['enzyme_name', 'organism_name']
    
    # get unique rows
    df = df.drop_duplicates()
    
    # require both organism and enzyme name
    df = df.dropna()
    # up to i=1100

    df = df[0:1100]
    content.append(df.copy())

    tried_df = pd.concat(content)
    tried_df = tried_df.drop_duplicates()
    tried_df.to_csv('fetch_sequences/uniprot/tried.tsv', sep='\t', index=False)



    exit(0)

if __name__ == '__main__':
    
    # create_tried()
    # df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')
    df = pd.read_csv('data/_compiled/apogee-all.tsv', sep='\t')
    # perform the rename enzyme_full --> enzyme_name, and organism --> organism_name

    # enzyme_preferred: enzyme_full, fill na with enzyme, then enzyme_2
    df['enzyme_preferred'] = df['enzyme_full'].fillna(df['enzyme']).fillna(df['enzyme_2'])
    df = df[['enzyme_preferred', 'organism']]
    df.columns = ['enzyme_name', 'organism_name']
    
    # get unique rows
    df = df.drop_duplicates()
    
    # require both organism and enzyme name
    df = df.dropna()

    tried_df = pd.read_csv('fetch_sequences/uniprot/tried.tsv', sep='\t')
    # drop rows which exactly match the tried ones

    df = df.merge(tried_df, on=['enzyme_name', 'organism_name'], how='left', indicator=True)
    df = df[df['_merge'] == 'left_only']
    df = df.drop(columns=['_merge'])

    
    
    # do in batches of 100, because I'm scared
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    # write_dest = 'fetch_sequences/uniprot/apogee-nonbrenda'
    write_dest = 'fetch_sequences/uniprot/apogee-brenda'
    print(f"To go: {len(df)}")
    for i in range(0, len(df), 100):
        # if i <= 17500:
            # continue # keep processing from where we left off
        result_df = process_dataframe(df[i:i+100])
        result_df.to_csv(f'{write_dest}/uniprot_{timestamp}_{i}.tsv', sep='\t', index=False)
        print(f'Written to {write_dest}/uniprot_{timestamp}_{i}.tsv')

        # update tried_df
        tried_df = pd.concat([tried_df, df[i:i+100]])
        tried_df = tried_df.drop_duplicates()
        tried_df.to_csv('fetch_sequences/uniprot/tried.tsv', sep='\t', index=False)
    