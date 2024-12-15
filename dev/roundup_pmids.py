# collect pmids from places once and for all


import os

import pandas as pd
from enzyextract.utils.pmid_doi_convert import find_canonical
from enzyextract.utils.pmid_management import cache_pmids_to_disk
from enzyextract.utils.pmid_management import pmids_from_cache
from enzyextract.utils.pmid_management import pmids_from_file
from enzyextract.utils.pmid_management import pmids_from_directory

# NOTE: doi to pmid mapping was originally provided at C:/conjunct/vandy/yang/dois/openaccess/create_openalex.py
# and C:/conjunct/vandy/yang/dois/openaccess/extract_wos_pmid_map.py
# result: C:/conjunct/vandy/yang/dois/openaccess/wos_nonelsevier_pmid_map.csv
# C:/conjunct/vandy/yang/dois/openaccess/wos_pmid_map.csv

def read_csv(filepath): # i wish pandas would just deal with int columns
    return pd.read_csv(filepath, dtype={'pmid': str, 'ids.pmid': str})


# def old_cache_pmids():
#     """
#     collect pmids/dois obtained from web of science
#     """
#     # wos dois
#     with open('C:/conjunct/vandy/yang/dois/summary/starting_dois.log', 'r') as f:
#         dois = f.readlines()
    
#     dois = set([x.strip().lower() for x in dois])

#     cache_pmids_to_disk(dois, 'all/wos_dois')

#     # wos pmids (excluding elsevier for now)
#     import pandas as pd
#     df = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_nonelsevier_pmid_map.csv')
#     # use pmid if it is present, otherwise doi
#     df['preferred'] = df['pmid'].fillna(df['doi'])
#     pmids = set(df['preferred'].dropna().astype(str).str.lower())
#     cache_pmids_to_disk(pmids, 'all/wos_not_elsevier')

#     # wiley
#     # simply load wos/wiley, topoff/wiley, scratch/wiley, brenda/wiley, and */hindawi
#     pmids = set()
#     subdirs = ['wos', 'topoff', 'scratch', 'brenda']
#     for subdir in subdirs:
#         pmids |= pmids_from_cache(f"{subdir}/wiley")
#         pmids |= pmids_from_cache(f"{subdir}/hindawi")
    
#     pmids = set([x.lower() for x in pmids])
#     cache_pmids_to_disk(pmids, 'all/wiley')
    

# def cache_pmids():
#     pass

# def roundup_wos_elsevier_1():
#     # all the dois from wos/elsevier

#     import pandas as pd
#     df = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_nonelsevier_pmid_map.csv')
#     # use pmid if it is present, otherwise doi
#     wos_nonelsevier_dois = set(df['doi'].dropna().astype(str).str.lower())

#     wos_dois = pmids_from_cache('all/wos_dois')
#     # i think these are not deduplicated
#     # lower them all
#     # wos_dois = set([x.lower() for x in wos_dois])

#     wos_elsevier_dois = wos_dois - wos_nonelsevier_dois
#     # len((wos_elsevier_dois & wos_nonelsevier_dois)) == 0

#     strange = (wos_elsevier_dois | wos_nonelsevier_dois) - wos_dois # 10371 are not in 
#     # the only weird one is 10.1073/pnas.2108648118 or 10.1073/pnas.2108648118|1of6
#     # which is in wos_nonelsevier_dois
#     cache_pmids_to_disk(wos_elsevier_dois, 'all/wos_elsevier_dois')

# def roundup_wos_elsevier_2():
#     # now that elsevier dois are converted via openalex, we can get the pmids

#     import pandas as pd
#     df1 = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_nonelsevier_pmid_map.csv')
#     df2 = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_elsevier_pmid_map.csv')

#     #df2 is mostly float, but it has some non-numeric pmids, let's take a look

#     # let's look for letters in df1['pmid']
#     weird = df1[df1['pmid'].str.contains('[a-zA-Z]', na=False)]
#     # remove those that start with PMC (which is not pmid)
#     df1 = df1[~df1['pmid'].isin(weird['pmid'])]

#     df1['pmid'] = df1['pmid'].astype('Int64').astype(str)
#     df2['pmid'] = df2['pmid'].astype('Int64').astype(str)

#     df = pd.concat([df1, df2])
#     df['preferred'] = df['pmid'].fillna(df['doi'])
#     pmids = set(df['preferred'].dropna().astype(str).str.lower())
#     cache_pmids_to_disk(pmids, 'all/wos')

# def roundup_filenames():
#     # round up all filenames downloaded, ever
#     # begin by iterating through all pdfs in brenda/**, scratch/**, topoff/**, wos/**
#     # then, iterate through all xmls in D:/wos/elsevier/xmls/**
#     # then, iterate through all xmls in C:\conjunct\vandy\yang\dois\elsevier\downloads
#     filenames = set()
#     for root in ['brenda', 'scratch', 'topoff', 'wos']:
#         # walk them
#         for root, dirs, files in os.walk(f'D:/{root}'):
#             for file in files:
#                 if file.endswith('.pdf'):
#                     # drop the extension
#                     filenames.add(file.rsplit('.', 1)[0])
    
#     for root in ['D:/wos/elsevier/xmls', 'C:/conjunct/vandy/yang/dois/elsevier/downloads']:
#         for root, dirs, files in os.walk(root):
#             for file in files:
#                 if file.endswith('.xml'):
#                     filenames.add(file.rsplit('.', 1)[0])
    
#     # wow... remove unicode filenames
#     unicode_filenames = set()
#     for filename in filenames:
#         try:
#             filename.encode('ascii')
#         except UnicodeEncodeError:
#             unicode_filenames.add(filename)
    
#     filenames -= unicode_filenames
    
#     cache_pmids_to_disk(filenames, 'all/downloaded_filenames')

# def roundup_discovery_openalex():
#     # round up those discovered by openalex
#     # AKA pmid in topoff, scratch, openelse (xml)
#     # use the csvs: C:\conjunct\vandy\yang\dois\openaccess\topoff_open_v2.csv
#     # C:\conjunct\vandy\yang\corpora\scratch\scratch_list_of_all_dois.csv
#     # C:\conjunct\vandy\yang\dois\openalex_from_scratch\vier_list_of_dois.csv

#     import pandas as pd
#     df1 = pd.read_csv('C:/conjunct/vandy/yang/dois/openaccess/topoff_open_v2.csv')
#     df2 = pd.read_csv('C:/conjunct/vandy/yang/corpora/scratch/scratch_list_of_all_dois.csv')
#     df3 = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex_from_scratch/vier_list_of_dois.csv')
#     pmids = set()
#     for df in [df1, df2, df3]:
#         if 'ids.pmid' in df.columns:
#             df = df.rename(columns={'ids.pmid': 'pmid'})
#         df['pmid'] = df['pmid'].astype('Int64').astype(str)
#         df['preferred'] = df['pmid'].fillna(df['doi'])
        
#         pmids |= set(df['preferred'].dropna().astype(str).str.lower())
#     cache_pmids_to_disk(pmids, 'all/openalex_discovery')

# def form_wos_pmid_map():
#     if not os.path.exists('C:/conjunct/vandy/yang/dois/openalex/wos_pmid_map.csv'):
#         import pandas as pd
#         df1 = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_nonelsevier_pmid_map.csv', dtype={'pmid': str})
#         df2 = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_elsevier_pmid_map.csv', dtype={'pmid': str})
#         df = pd.concat([df1, df2])
#         df.to_csv('C:/conjunct/vandy/yang/dois/openalex/wos_pmid_map.csv', index=False)

# def roundup_download_openalex():
#     # collect pmids downloaded by openalex
#     # 1a. scratch: read 'pmid' or 'doi' from C:\conjunct\vandy\yang\corpora\scratch\scratch_list_of_all_dois.csv
#     # 1b. topoff: read 'pmid' or 'doi' from C:\conjunct\vandy\yang\corpora\topoff\topoff_open_v2.csv
#     # 2. wos: read 'pmid' or 'doi' from C:\conjunct\vandy\yang\corpora\manifest\wos_open_all_v2.csv
#     # 3. brenda: read 'pmid' or 'doi' from C:\conjunct\vandy\yang\corpora\brenda\get_dois\by_source\brenda_list_of_open_dois.csv

#     import pandas as pd

#     # make a pmid to df dataframe
#     scratch_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/scratch/scratch_list_of_all_dois.csv',
#                             dtype={'pmid': str})
#     topoff_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/topoff/topoff_open_v2.csv',
#                             dtype={'pmid': str})
#     # wos_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/manifest/wos_open_all_v2.csv',
#     #                      dtype={'pmid': str})

#     # note that none of the wos have pmids

#     wos_df = pd.read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_pmid_map.csv',
#                             dtype={'pmid': str})
#     brenda_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/brenda/get_dois/by_source/brenda_list_of_open_dois.csv',
#                             dtype={'pmid': str})
#     builder = []
#     for df in [scratch_df, topoff_df, wos_df, brenda_df]:
#         if 'ids.pmid' in df.columns:
#             df = df.rename(columns={'ids.pmid': 'pmid'})
#         weird = df[df['pmid'].str.startswith('PMC', na=False)]
#         # remove those that start with PMC (which is not pmid)
#         df = df[~df['pmid'].isin(weird['pmid'])]
#         df['preferred'] = df['pmid'].fillna(df['doi'].str.lower())
#         df = df.dropna(subset=['preferred'])
#         # apply doi_management.doi_to_filename
#         from  enzyextract.utils.doi_management import doi_to_filename
#         df['filename'] = df['preferred'].apply(lambda x: doi_to_filename(x, file_extension=''))
#         builder.append(df[['filename', 'preferred']])


#     filenames = set()
#     for subdir in ['asm', 'open', 'open_remote_scratch']:
#         filenames |= pmids_from_cache(f'scratch/{subdir}')
    
#     for subdir in ['open']:
#         filenames |= pmids_from_cache(f'topoff/{subdir}')

#     for subdir in ['asm', 'jbc', 'local_shim', 'open', 'remote_goldgreen_kinetic', 'remote_pdfs_bronze', 'remote_pdfs_hybrid', 'remote_shim']:
#         filenames |= pmids_from_cache(f'wos/{subdir}')

#     filenames = set([x.lower() for x in filenames])

#     # convert filename to pmid
#     filename_to_pmid = {}
#     for df in builder:
#         for row in df.itertuples():
#             filename_to_pmid[row.filename] = row.preferred
#     pmids = set()

#     not_sure_where = set()
#     from enzyextract.utils.doi_management import filename_to_doi
#     for filename in filenames:
#         if filename in filename_to_pmid:
#             pmids.add(filename_to_pmid[filename])
#         else:
#             # pmids.add(filename_to_doi(filename))
#             not_sure_where.add(filename)

#     cache_pmids_to_disk(pmids, 'all/openalex_download')
#     cache_pmids_to_disk(not_sure_where, 'all/openalex_download_not_sure_where')


def canonicalize(dois):
    canonical = set()
    uncanonical = set()
    for doi in dois:
        doi = doi.lower() # take no chances
        result = find_canonical(doi)
        if result is None:
            uncanonical.add(doi)
        else:
            canonical.add(result)
    return canonical, uncanonical

def roundup_discovery():
    # round up all methods of discovery (wos, brenda, openalex)
    # wos_df = read_csv('C:/conjunct/vandy/yang/dois/openalex/wos_pmid_map.csv')
    # wos_df['preferred'] = wos_df['pmid'].fillna(wos_df['doi']).str.lower()
    canonical = {}
    uncanonical = {}


    wos_dois = pmids_from_file('C:/conjunct/vandy/yang/corpora/manifest/wos_all.log')
    # note that these might not be lower
    wos_dois = set([x.lower() for x in wos_dois])
    wos_ok, wos_missing = canonicalize(wos_dois)
    canonical['wos'] = wos_ok
    uncanonical['wos'] = wos_missing
    
    brenda_dois = pmids_from_cache('brenda')
    brenda_ok, brenda_missing = canonicalize(brenda_dois)
    canonical['brenda'] = brenda_ok
    uncanonical['brenda'] = brenda_missing

    openalex_dois = pmids_from_cache('all/openalex_discovery')
    openalex_ok, openalex_missing = canonicalize(openalex_dois)
    canonical['openalex'] = openalex_ok
    uncanonical['openalex'] = openalex_missing
    
    # len(canonical['wos']) == 36001
    # len(uncanonical['wos']) == 95516
    # len(uncanonical['wos']) == 1671 now, which represent those known to wos but unknown
    # to openalex
    # uncanonical brenda and openalex: empty

    # now, construct the canonical df
    builder = []
    for source, pmids in canonical.items():
        for pmid in pmids:
            builder.append({'canonical': pmid, 'source': source})
    df1 = pd.DataFrame(builder)
    # df1.to_csv('dev/data/pmids/discovery/discovery_canonical.csv', index=False)

    builder = []
    for source, pmids in uncanonical.items():
        for pmid in pmids:
            builder.append({'canonical': pmid, 'source': source})
    df2 = pd.DataFrame(builder)
    df2.to_csv('dev/data/pmids/discovery/discovery_uncanonical.csv', index=False)

    df = pd.concat([df1, df2])
    df.to_csv('dev/data/pmids/discovery/discovery_all.csv', index=False)

    # uncanonical: 1671 --> 1671
    pass

def roundup_download():
    # round up pmids by download method
    # convert to canonical, which is super important because there are filenames

    # scihub
    # these should all be ints
    unknown = {}
    downloaded = {}
    downloaded['scihub'] = pmids_from_directory('D:/brenda/scihub', filetype='.pdf')

    # wiley
    downloaded['wiley'] = set()
    wiley_files = pmids_from_directory('D:/brenda/wiley', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/brenda/hindawi', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/scratch/wiley', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/scratch/hindawi', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/topoff/wiley', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/topoff/hindawi', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/wos/wiley', filetype='.pdf')
    wiley_files |= pmids_from_directory('D:/wos/hindawi', filetype='.pdf')

    # canonicalize
    canonical, uncanonical = canonicalize(wiley_files)
    downloaded['wiley'] = canonical
    unknown['wiley'] = uncanonical

    # openalex
    locations = {
        'scratch': ['asm', 'open', 'open_remote'],
        'topoff': ['open'],
        'wos': ['asm', 'jbc', 'local_shim', 'open', 'remote/goldgreen_kinetic', 'remote/pdfs_bronze', 'remote/pdfs_hybrid', 'remote/shim'],
        'brenda': ['asm', 'jbc', 'open', 'pnas']
    }
    filenames = set()
    for toplevel, subdirs in locations.items():
        for subdir in subdirs:
            filenames |= pmids_from_directory(f'D:/{toplevel}/{subdir}', filetype='.pdf')
    canonical, uncanonical = canonicalize(filenames)
    downloaded['openalex'] = canonical
    unknown['openalex'] = uncanonical

    # elsevier
    elsevier_files = pmids_from_directory('D:/openelse/downloads', filetype='.xml')
    elsevier_files |= pmids_from_directory('D:/wos/elsevier/xmls', filetype='.xml', recursive=True)

    canonical, uncanonical = canonicalize(elsevier_files)
    downloaded['elsevier'] = canonical
    unknown['elsevier'] = uncanonical

    # construct dfs
    # unknown: 10.1897_1552-8618(1994)13[1957%3Aeaftwa]2.0.co;2
    # actual : 10.1897/1552-8618(1994)13[1957:eaftwa]2.0.co;2
    builder = []
    for source, pmids in downloaded.items():
        for pmid in pmids:
            builder.append({'canonical': pmid, 'source': source})
    canonical_df = pd.DataFrame(builder)

    builder = []
    for source, pmids in unknown.items():
        for pmid in pmids:
            builder.append({'canonical': pmid, 'source': source})
    unknown_df = pd.DataFrame(builder)
    all_df = pd.concat([canonical_df, unknown_df])
    all_df.to_csv('dev/data/pmids/download/download_all.csv', index=False)
    unknown_df.to_csv('dev/data/pmids/download/download_unknown.csv', index=False)

    # unknown:
    # wiley: 3 --> 0
    # openalex: 197 --> 0
    # elsevier: 561 --> 556 (not known to openalex)

    pass


def prepare_sankey():
    # stage 1: keyword discovery
    # stage 2: download

    discovery_df = pd.read_csv('dev/data/pmids/discovery/discovery_all.csv')
    download_df = pd.read_csv('dev/data/pmids/download/download_all.csv', dtype={'canonical': str})

    # pmids = download_df['canonical']
    # # reject those that have unicode
    # pmids = [x for x in pmids if x.encode('ascii', 'ignore').decode('ascii') == x]

    # cache_pmids_to_disk(pmids, 'downloaded')

    discovery_df.dropna(subset=['canonical'], inplace=True)
    download_df.dropna(subset=['canonical'], inplace=True)

    # target format:
    
    # Wages [1500] Budget
    # Other [250] Budget

    # Budget [450] Taxes
    # Budget [420] Housing
    # Budget [400] Food
    # Budget [295] Transportation
    # Budget [25] Savings

    # https://sankeymatic.com/build/

    builder = ""
    discovery_cats = sorted(set(discovery_df['source']))

    # print category and count
    # dest: Discovered
    for category in discovery_cats:
        count = len(discovery_df[discovery_df['source'] == category])
        builder += f"{category} [{count}] Discovered\n"

    # calculate number unique
    keyword_total = len(discovery_df['canonical'].dropna())
    keyword_unique = len(set(discovery_df['canonical'].dropna()))

    # calculate number of duplicates
    # keyword['num_duplicates'] = keyword['num_total'] - keyword['num_unique']
    keyword_duplicates = keyword_total - keyword_unique
    # builder += f"Discovered [{discovered['num_total']}] Total\n"

    
    discovery_cats = sorted(set(download_df['source']))
    # print category and count
    # dest: Downloaded

    total_downloaded = 0
    for category in discovery_cats:
        count = len(download_df[download_df['source'] == category])
        if category == 'openalex':
            category = 'openalex_download'
        builder += f"Discovered [{count}] {category}\n"
        total_downloaded += count
    
    # calculate number unique
    dl_total = len(download_df)
    assert dl_total == total_downloaded, f"{dl_total} != {total_downloaded}"
    dl_unique_df = download_df.drop_duplicates(subset=['canonical'])
    dl_unique = len(dl_unique_df)
    dl_duplicates = dl_total - dl_unique
    
    num_not_downloaded = keyword_unique - dl_unique
    builder += f"Discovered [{num_not_downloaded}] Not Downloaded\n"
    builder += f"Discovered [{keyword_duplicates}] Discovered twice\n"

    # do math to figure how many we downloaded twice
    all_discovered = keyword_total
    all_downloaded_or_skipped = dl_total + num_not_downloaded
    print(total_downloaded, num_not_downloaded, all_discovered, all_downloaded_or_skipped)
    # 220833 206754
    # want 223185
    builder += f"Downloaded twice [{dl_duplicates}] Discovered\n"

    return builder


def prepare_sankey_2():
        # stage 1: keyword discovery
    # stage 2: download

    # LHS
    discovery_df = pd.read_csv('dev/data/pmids/discovery/discovery_all.csv', dtype={'canonical': str})

    # RHS
    download_df = pd.read_csv('dev/data/pmids/download/download_all.csv', dtype={'canonical': str})

    # make things easier: deduplicate the LHS (discovery)
    if True:
        # Define priority order for sources: brenda > openalex > wos
        priority_order = {'brenda': 1, 'openalex': 2, 'wos': 3}
        discovery_df['priority'] = discovery_df['source'].map(priority_order)

        # Sort by priority and drop duplicates by 'canonical', keeping the first occurrence
        discovery_df.sort_values(by='priority', inplace=True)
        discovery_df = discovery_df.drop_duplicates(subset=['canonical'], keep='first')

        # Remove the 'priority' column as it’s no longer needed
        discovery_df.drop(columns=['priority'], inplace=True)

        # Define priority order for download sources: wiley > openalex > elsevier > scihub
        priority_order = {'wiley': 1, 'openalex': 2, 'elsevier': 3, 'scihub': 4}
        download_df['priority'] = download_df['source'].map(priority_order)
        download_df.sort_values(by='priority', inplace=True)
        download_df = download_df.drop_duplicates(subset=['canonical'], keep='first')
        download_df.drop(columns=['priority'], inplace=True)

    # Drop rows with missing values in the 'canonical' column
    discovery_df.dropna(subset=['canonical'], inplace=True)
    download_df.dropna(subset=['canonical'], inplace=True)

    discovery_renames = {
        'wos': 'Web of Science',
        'brenda': 'BRENDA',
        'openalex': 'OpenAlex'
    }

    download_renames = {
        'openalex': 'OpenAlex API',
        'wiley': 'Wiley API',
        'scihub': 'scihub',
        'elsevier': 'Elsevier API'
    }

    # rename openalex on the RHS to openalex_download
    # download_df['source'] = download_df['source'].apply(lambda x: 'openalex_download' if x == 'openalex' else x)
    download_df['source'] = download_df['source'].map(lambda x: download_renames.get(x, x))
    discovery_df['source'] = discovery_df['source'].map(lambda x: discovery_renames.get(x, x))

    builder = ""
    
    # Calculate unique and duplicate counts for discovery
    keyword_total = len(discovery_df['canonical'])
    keyword_unique = len(discovery_df['canonical'].unique())
    keyword_duplicates = keyword_total - keyword_unique

    # Create a dictionary to count LHS -> RHS flows
    lhs_rhs_counts = {}

    # Count discovered sources on the LHS (e.g., brenda, openalex, wos)
    for category in discovery_df['source'].unique():
        # Filter rows by the LHS category
        category_df = discovery_df[discovery_df['source'] == category]
        downloaded_in_lhs_cat = download_df['canonical'].isin(category_df['canonical'])

        # Calculate how many of these entries go to each download source on the RHS
        for rhs_category in download_df['source'].unique():
            
            count = len(download_df[downloaded_in_lhs_cat & (download_df['source'] == rhs_category)])
            lhs_rhs_counts[(category, rhs_category)] = count
            if count != 0:
                builder += f"{category} [{count}] {rhs_category}\n"
    
    # Calculate "Not Downloaded" entries for each discovery source (LHS)
    downloaded_canons = set(download_df['canonical'])
    for category in discovery_df['source'].unique():
        category_df = discovery_df[discovery_df['source'] == category]
        discovered_canons = set(category_df['canonical'])
        
        # not_downloaded_count = len(discovered_canons - downloaded_canons)

        dnd = discovered_canons & downloaded_canons

        not_downloaded_count = len(category_df) - len(dnd)
        
        builder += f"{category} [{not_downloaded_count}] Not Downloaded\n"

    # Add duplicates counts for each download category
    dl_total = len(download_df)
    dl_unique = len(download_df['canonical'].unique())
    dl_duplicates = dl_total - dl_unique
    builder += f"Downloaded twice [{dl_duplicates}] Discovered\n"
    
    # Add counts for "Discovered twice" and other relevant fields
    builder += f"Discovered [{keyword_duplicates}] Discovered twice\n"

    return builder
    

if __name__ == "__main__":
    # roundup_discovery()
    # roundup_download()
    # exit(0)

    builder = prepare_sankey_2()
    print(builder)

    pass


