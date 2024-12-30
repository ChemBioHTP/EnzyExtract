import requests
import json

    # entryType
    # primaryAccession
    # uniProtkbId
    # organism
    #   scientificName
    #   commonName
    #   taxonId
    #   evidences: [...]
    #   lineage: [...]
    # proteinDescription
    #   recommendedName?
    #     fullName?
    #       value?
    #   submissionNames
    #     fullName?
    #       value?


def _fetch_uniprot_info_expanded(uniprot_ids):
    # Use UniProt API to fetch information in bulk
    url = "https://rest.uniprot.org/uniprotkb/search"
    query = " OR ".join(uniprot_ids)
    params = {
        "query": query,
        "fields": "accession,id,protein_name,organism_name,sequence,ec", # lit_pubmed_id
        "format": "json"
    } # https://www.uniprot.org/help/return_fields
    response = requests.get(url, params=params)
    return response.json()


import polars as pl

def fetch_uniprots_expanded(uniprot_ids: list, do_redirects=True) -> pl.DataFrame:
    """
    Columns:
    ['uniprot', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'uniparc', 'why_deleted']
    """
    results = []
    uniprot_info = _fetch_uniprot_info_expanded(uniprot_ids)
    if 'results' not in uniprot_info:
        print("Unexpected format")
        print(uniprot_info)
    # https://rest.uniprot.org/uniprotkb/search?query=O43520
    
    results = {
        'uniprot': [],
        'enzyme_name': [],
        'organism': [],
        'organism_common': [],
        'sequence': [],
        'ec_numbers': [],
        'uniparc': [],
        'why_deleted': [],
        
    }
    found_ids = set()
    for entry in uniprot_info['results']:
        uniprot_id = entry.get('primaryAccession', "not found")
        results['uniprot'].append(uniprot_id)
        found_ids.add(uniprot_id)

        enzyme = entry.get('proteinDescription', {}).get('recommendedName', {}).get('fullName', {}).get('value', None)
        if enzyme is None:
            enzyme = (entry.get('proteinDescription', {}).get('submissionNames') or [{}])[0].get('fullName', {}).get('value', None)
        results['enzyme_name'].append(enzyme)

        organism = entry.get('organism', {}).get('scientificName', None)
        results['organism'].append(organism)

        organism_common = entry.get('organism', {}).get('commonName', None)
        results['organism_common'].append(organism_common)

        sequence = entry.get('sequence', {}).get('value', None)
        results['sequence'].append(sequence)
        
        uniparc = entry.get('extraAttributes', {}).get('uniParcId', None)
        results['uniparc'].append(uniparc)

        if entry.get('entryType') == 'Inactive':
            if entry.get('inactiveReason', {}).get('inactiveReasonType') == 'DELETED':
                results['why_deleted'].append(entry.get('inactiveReason', {}).get('deletedReason', 'deleted'))
            else:
                results['why_deleted'].append('unknown')
        else:
            results['why_deleted'].append(None)


        ec_numbers = []
        for submission in entry.get('proteinDescription', {}).get('submissionNames', []):
            ec = submission.get('ecNumbers', [])
            ec = [x.get('value') for x in ec]
            ec_numbers += ec
        results['ec_numbers'].append(ec_numbers)
    

        # results.append((uniprot_id, enzyme, organism, sequence))
    # to df
    # if redirects and do_redirects:
        # print("Redirecting", redirects)
        # results += fetch_uniprots_expanded(redirects, do_redirects=False)
    # df = pd.DataFrame(results, columns=['uniprot', 'enzyme', 'organism', 'sequence'])
    # ['accession', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'uniparc', 'why_deleted']
    df = pl.DataFrame(results, schema_overrides={
        'uniprot': pl.Utf8,
        'enzyme_name': pl.Utf8,
        'organism': pl.Utf8,
        'organism_common': pl.Utf8,
        'sequence': pl.Utf8,
        'ec_numbers': pl.List(pl.Utf8),
        'uniparc': pl.Utf8,
        'why_deleted': pl.Utf8,
    })
    return df


# P49021



def _fetch_uniparc_info(uniprot_ids):
    # Use UniProt API to fetch information in bulk
    url = "https://rest.uniprot.org/uniprotkb/search"
    query = " OR ".join(uniprot_ids)
    params = {
        "query": query,
        "fields": "accession,id,protein_name,organism_name,sequence,ec", # lit_pubmed_id
        "format": "json"
    } # https://www.uniprot.org/help/return_fields
    response = requests.get(url, params=params)

    return response.json()

def fetch_uniparc(uniprot_ids: list, do_redirects=True) -> pl.DataFrame:
    raise NotImplementedError

def _fetch_uniprot_one(uniprot_id: str):
    # Use UniProt API to fetch one
    url = "https://rest.uniprot.org/uniprotkb/" + uniprot_id
    # https://www.uniprot.org/help/return_fields
    params = {
        "format": "json"
    }
    # https://rest.uniprot.org/uniprotkb/A0A0M9G714
    
    response = requests.get(url, params=params)
    # if 404, return {}
    if response.status_code == 404:
        return {}
    return response.json()


def extract_uniprotkb_fields(entry: dict, results: dict, want_uniprot: str=None):
    """
    expects these keys in results:
    ['query_uniprot', 'uniprot', 'uniprot_aliases', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'pmids', 'dois', 'uniparc', 'why_deleted', 'full_response']
    """
    uniprot_id = entry.get('primaryAccession', None)
    results['query_uniprot'].append(want_uniprot)
    results['uniprot'].append(uniprot_id)

    uniprot_aliases = entry.get('secondaryAccessions', [])
    # will be added later

    enzyme = entry.get('proteinDescription', {}).get('recommendedName', {}).get('fullName', {}).get('value', None)
    if enzyme is None:
        enzyme = (entry.get('proteinDescription', {}).get('submissionNames') or [{}])[0].get('fullName', {}).get('value', None)
    if enzyme is None:
        enzyme = (entry.get('proteinDescription', {}).get('alternativeNames') or [{}])[0].get('fullName', {}).get('value', None)
    results['enzyme_name'].append(enzyme)

    organism = entry.get('organism', {}).get('scientificName', None)
    results['organism'].append(organism)

    organism_common = entry.get('organism', {}).get('commonName', None)
    results['organism_common'].append(organism_common)

    sequence = entry.get('sequence', {}).get('value', None)
    results['sequence'].append(sequence)
    
    uniparc = entry.get('extraAttributes', {}).get('uniParcId', None)
    results['uniparc'].append(uniparc)

    if entry.get('entryType') == 'Inactive':
        inactiveReasonType = entry.get('inactiveReason', {}).get('inactiveReasonType', None)
        if inactiveReasonType == 'DELETED':
            results['why_deleted'].append(entry.get('inactiveReason', {}).get('deletedReason', 'deleted'))
        elif inactiveReasonType == 'DEMERGED':
            results['why_deleted'].append('demerged')
            for demerged in entry.get('inactiveReason', {}).get('mergeDemergeTo', []):
                uniprot_aliases.append(demerged)
        elif inactiveReasonType == 'MERGED':
            results['why_deleted'].append('merged')
            for merged in entry.get('inactiveReason', {}).get('mergeDemergeTo', []):
                uniprot_aliases.append(merged)
        else:
            results['why_deleted'].append('unknown')
    else:
        results['why_deleted'].append(None)
    

    ec_numbers = []
    for submission in entry.get('proteinDescription', {}).get('submissionNames', []):
        ec = submission.get('ecNumbers', [])
        ec = [x.get('value') for x in ec]
        ec_numbers += ec
    results['ec_numbers'].append(ec_numbers)

    dois = []
    pmids = []
    for lit in entry.get('references', []):
        if 'citation' in lit:
            if 'citationCrossReferences' in lit['citation']:
                for xref in lit['citation']['citationCrossReferences']:
                    if xref['database'] == 'DOI':
                        dois.append(xref['id'])
                    elif xref['database'] == 'PubMed':
                        pmids.append(xref['id'])
    results['dois'].append(dois)
    results['pmids'].append(pmids)

    results['uniprot_aliases'].append(uniprot_aliases)

    # convert entry to str
    results['full_response'].append(json.dumps(entry))

def fetch_uniprots_individually(uniprot_ids: list) -> pl.DataFrame:
    """
    Columns:
    ['uniprot', 'uniprot_aliases', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'pmids', 'dois', 'uniparc', 'why_deleted', 'full_response']
    """
    from tqdm import tqdm
    import time
    # https://rest.uniprot.org/uniprotkb/O43520
    results = {
        'query_uniprot': [],
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
    for want_uniprot in tqdm(uniprot_ids):
        entry = _fetch_uniprot_one(want_uniprot)
        # for entry in uniprot_info['results']:
        extract_uniprotkb_fields(entry, results, want_uniprot)
        time.sleep(0.5)
    
    
    df = pl.DataFrame(results, schema_overrides={
        'query_uniprot': pl.Utf8,
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

def _fetch_uniprot_info_latest(uniprot_ids):
    # Use UniProt API to fetch information in bulk
    url = "https://rest.uniprot.org/uniprotkb/search"
    query = " OR ".join(f'(accession:{x})' for x in uniprot_ids)
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


import polars as pl

def fetch_uniprots_latest(uniprot_ids: list) -> pl.DataFrame:
    """
    Columns:
    ['uniprot', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'uniparc', 'why_deleted']
    """
    results = []
    uniprot_info = _fetch_uniprot_info_latest(uniprot_ids)
    if 'results' not in uniprot_info:
        print("Unexpected format")
        print(uniprot_info)
    # https://rest.uniprot.org/uniprotkb/search?query=accession:O43520
    # https://rest.uniprot.org/uniprotkb/search?query=accession:P62988
    results = {
        'query_uniprot': [],
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
        extract_uniprotkb_fields(entry, results)
        
    df = pl.DataFrame(results, schema_overrides={
        'query_uniprot': pl.Utf8,
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



def fetch_uniprots_from_pmids(pmids: list) -> pl.DataFrame:
    """
    Columns:
    ['uniprot', 'enzyme_name', 'organism', 'organism_common', 'sequence', 'ec_numbers', 'uniparc', 'why_deleted']
    """
    results = []
    url = "https://rest.uniprot.org/uniprotkb/search"
    query = " OR ".join(f'(lit_pubmed:{x})' for x in pmids)
    params = {
        "query": query,
        # "fields": "accession,id,protein_name,organism_name,sequence,ec", # lit_pubmed_id
        "format": "json",
        "size": 500
    } # https://www.uniprot.org/help/return_fields
    # https://rest.uniprot.org/uniprotkb/search/?query=lit_pubmed:17418235
    uniprot_info = {}
    for response, total in get_batch(url, params):
        data = response.json()
        if "results" not in uniprot_info:
            uniprot_info["results"] = data.get("results", [])
            uniprot_info["total"] = total
        else:
            uniprot_info["results"] += data.get("results", [])
            uniprot_info["total"] += total
    
    if 'results' not in uniprot_info:
        print("Unexpected format")
        print(uniprot_info)
    results = {
        'query_uniprot': [],
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
        extract_uniprotkb_fields(entry, results)
        
    df = pl.DataFrame(results, schema_overrides={
        'query_uniprot': pl.Utf8,
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

