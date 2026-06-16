import json
import os
from pathlib import Path
import polars as pl
import time
from tqdm import tqdm
import urllib

if __name__ == "__main__":
    raise ValueError("Please provide an email address.")
email = "xxx" # replace with your email
# Define the URL and headers
headers = {
    # 'Accept': 'application/vnd.crossref.unixsd+xml',
    'User-Agent': f'EnzyExtractor/1.0 (mailto:{email})'
}



def extract_openalex(dois: list[str]):
    import requests

    dois = [urllib.parse.quote(doi, safe='/', encoding=None, errors=None) for doi in dois]
    pipe_separated_dois = "|".join(dois)

    url = f"https://api.openalex.org/works?filter=doi:{pipe_separated_dois}&per-page=50&mailto=support@openalex.org"
    
    response = requests.get(url, headers=headers, allow_redirects=True)
    if response.status_code != 200:
        return response, None
    works = response.json()["results"]

    return response, works


def script1_obtain_bib_data(
    dois: list[str],
    dest_file: Path,
    log_file: Path,
):

    # get line number from log file to resume from there
    last_processed = 0
    with open(log_file, 'a') as log:
        for i in tqdm(range(last_processed, len(dois), 50)):
            chunk = dois[i:i+50]
            try:
                response, json_obj = extract_openalex(chunk)
            except Exception as e:
                print(e)
                log.write(f"EXCEP {chunk}\n")
                log.flush()
                continue
            if response.status_code != 200:
                log.write(f"ERROR {chunk}\n")
                log.flush()
                continue
            with open(dest_file, 'ab') as f:
                f.write(response.content)
                # f.write(b'\n')
                f.flush()

            # sleep 0.5s to avoid rate limiting
            # 50/s at maximum
            time.sleep(0.2)


def collect_openalex_dict(inp):
    """
    turn openalex dict into a record suitable for polars
    """
    _id = inp['id']
    if _id.startswith('https://openalex.org/'):
        _id = _id[len('https://openalex.org/'):]
    _doi = inp.get('doi', None)
    if _doi and _doi.startswith('https://doi.org/'):
        _doi = _doi[len('https://doi.org/'):]
    _pmid = inp.get('ids', {}).get('pmid', None)
    if _pmid and _pmid.startswith('https://pubmed.ncbi.nlm.nih.gov/'):
        _pmid = _pmid[len('https://pubmed.ncbi.nlm.nih.gov/'):]
    
    _best_oa_location = inp.get('best_oa_location', {}) or {}
    ret = {
        'id': _id,
        'doi': _doi,
        'pmid': _pmid, # an insert
        'title': inp.get('title', None),
        'display_name': inp.get('display_name', None),
        'publication_year': inp.get('publication_year', None),
        'publication_date': inp.get('publication_date', None),
        # ids.openalex
        # ids.doi
        'ids.mag': inp.get('ids', {}).get('mag', None),
        'ids.pmid': _pmid,
        'language': inp.get('language', None),
        'primary_location.is_oa': inp.get('primary_location', {}).get('is_oa', None),
        'primary_location.landing_page_url': inp.get('primary_location', {}).get('landing_page_url', None),
        'primary_location.pdf_url': inp.get('primary_location', {}).get('pdf_url', None),
        # primary_location.source
        'primary_location.license': inp.get('primary_location', {}).get('license', None),
        'primary_location.license_id': inp.get('primary_location', {}).get('license_id', None),
        'primary_location.version': inp.get('primary_location', {}).get('version', None),
        'primary_location.is_accepted': inp.get('primary_location', {}).get('is_accepted', None),
        'primary_location.is_published': inp.get('primary_location', {}).get('is_published', None),
        'type': inp.get('type', None),
        'type_crossref': inp.get('type_crossref', None),
        'indexed_in': (inp.get('indexed_in') or []),
        'open_access.is_oa': inp.get('open_access', {}).get('is_oa', None),
        'open_access.oa_status': inp.get('open_access', {}).get('oa_status', None),
        'open_access.oa_url': inp.get('open_access', {}).get('oa_url', None),
        'open_access.any_repository_has_fulltext': inp.get('open_access', {}).get('any_repository_has_fulltext', None),
        # authorships
        # countries_distinct_count
        # institutions_distinct_count
        # corresponding_author_ids
        # corresponding_institution_ids
        # apc_list
        # apc_paid
        'has_fulltext': inp.get('has_fulltext', None),
        'fulltext_origin': inp.get('fulltext_origin', None),
        'cited_by_count': inp.get('cited_by_count', None),
        # cited_by_percentile_year
        # biblio
        'is_retracted': inp.get('is_retracted', None),
        'is_paratext': inp.get('is_paratext', None),
        # primary_topic.*
        # topics.*
        # keywords.*
        # concepts.*
        # mesh.*
        'locations_count': inp.get('locations_count', None),
        'locations': inp.get('locations', None), # could be messy
        'best_oa_location.is_oa': _best_oa_location.get('is_oa', None),
        'best_oa_location.landing_page_url': _best_oa_location.get('landing_page_url', None),
        'best_oa_location.pdf_url': _best_oa_location.get('pdf_url', None),
        # best_oa_location.source.*
        'best_oa_location.license': _best_oa_location.get('license', None),
        'best_oa_location.license_id': _best_oa_location.get('license_id', None),
        'best_oa_location.version': _best_oa_location.get('version', None),
        'best_oa_location.is_accepted': _best_oa_location.get('is_accepted', None),
        'best_oa_location.is_published': _best_oa_location.get('is_published', None),

        # sustainable_development_goals
        # grants
        # datasets
        # versions
        # referenced_works_count
        # referenced_works
        # related_works
        'ngrams_url': inp.get('ngrams_url', None),
        # abstract_inverted_index
        # cited_by_api_url
        # counts_by_year
        'updated_date': inp.get('updated_date', None),
        'created_date': inp.get('created_date', None),
    }
    return ret

def script2_compress_to_parquet(jsonl_file: Path, parquet_file: Path):
    """
    compress json files to parquet
    """
    
    collector = []
    with open(jsonl_file) as f:
        for line in f:
            objs = json.loads(line)['results']
            for obj in objs: 
                # df = pl.DataFrame(obj['results'])
                collector.append(collect_openalex_dict(obj))

    df = pl.DataFrame(collector)

    df.write_parquet(parquet_file)


if __name__ == "__main__":

    # Get dois
    dois = [] # Insert dois here

    dest_file = Path('data/bib/topoff_redone.jsonl')
    log_file = Path('data/bib/topoff_redone.log')

    script1_obtain_bib_data(dois, dest_file, log_file)

    parquet_file = Path('data/bib/topoff_redone.parquet')
    script2_compress_to_parquet(dest_file, parquet_file)
