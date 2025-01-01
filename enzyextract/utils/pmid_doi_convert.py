"""
Very specialized module to convert between doi and pmid, to clean up legacy files stored using dois.
Very hacky, there should be no need for this module if you get it right the first time and store everything using PMID preferentially, then DOI.

"""

import pandas as pd

from enzyextract.utils.doi_management import doi_to_filename
def _read_csv(filepath):
    return pd.read_csv(filepath, dtype={'pmid': str, 'ids.pmid': str})
def _roundup_doi_pmid_dict():
    wos_df = _read_csv(r'C:/conjunct/vandy/yang/dois/openalex/wos_pmid_map.csv')

    scratch_df = _read_csv(r'C:\conjunct\vandy\yang\corpora\scratch\scratch_list_of_all_dois.csv')

    topoff_df = _read_csv(r'C:\conjunct\vandy\yang\corpora\topoff\topoff_open_v2.csv')

    brenda_df = _read_csv(r'C:\conjunct\vandy\yang\corpora\brenda\get_dois\brenda_list_of_dois.csv')

    elsevier_df = _read_csv(r'C:\conjunct\vandy\yang\dois\openalex_from_scratch\vier_list_of_dois.csv')

    builder = []
    for df, source in [(wos_df, 'wos'), (scratch_df, 'scratch'), (topoff_df, 'topoff'), (brenda_df, 'brenda'), (elsevier_df, 'openelse')]:
        df = df.rename(columns={'ids.pmid': 'pmid'})
        df['source'] = source
        builder.append(df[['doi', 'pmid', 'source']])
    
    doi_pmid_df = pd.concat(builder, ignore_index=True)
    # standardization: doi to lowercase
    doi_pmid_df['doi'] = doi_pmid_df['doi'].str.lower()
    doi_pmid_df = doi_pmid_df.drop_duplicates(subset=['doi', 'pmid']) # drop those all null

    # calculate doi_filename
    doi_pmid_df['doi_filename'] = doi_pmid_df['doi'].apply(lambda x: doi_to_filename(x, file_extension=''))

    # calculate preferred: pick pmid. if null, then go for doi
    doi_pmid_df['preferred'] = doi_pmid_df['pmid'].fillna(doi_pmid_df['doi'])

    # drop null preferred
    doi_pmid_df = doi_pmid_df.dropna(subset=['preferred'])

    doi_pmid_df.to_csv(r'data/pmids/doi_pmid_dict_1.csv', index=False)

doi_pmid_df = None
filename_to_canonical = {}
doi_to_canonical = {}
def _load_doi_pmid_dict():
    global doi_pmid_df, filename_to_canonical, doi_to_canonical
    if doi_pmid_df is not None:
        return
    doi_pmid_df = pd.read_csv(r'data/pmids/doi_pmid_dict_1.csv')
    filename_to_canonical = {}
    doi_to_canonical = {}
    for doi, doi_filename, preferred in doi_pmid_df[['doi', 'doi_filename', 'preferred']].values:
        filename_to_canonical[doi_filename] = preferred
        doi_to_canonical[doi] = preferred



def find_canonical(filename: str, default=None):
    _load_doi_pmid_dict()
    filename = filename.lower()
    global doi_pmid_df, filename_to_canonical, doi_to_canonical
    # if completely numerical, then it will be canonical
    if filename.isnumeric():
        return filename
    
    if filename.endswith('.pdf') or filename.endswith('.xml'):
        filename = filename[:-4]

    if filename in filename_to_canonical:
        return filename_to_canonical[filename]
    # check if it's a doim tbe  return
    if filename in doi_to_canonical:
        return doi_to_canonical[filename]
    if filename.isnumeric():
        return filename
    # don't know
    return default


if __name__ == '__main__':
    _roundup_doi_pmid_dict()