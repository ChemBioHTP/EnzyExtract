# working_enzy_table_md, but tableless

import io
import json
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from kcatextract.utils import prompt_collections
from kcatextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from kcatextract.utils.fresh_version import next_available_version
from kcatextract.utils.micro_fix import mM_corrected_text
from kcatextract.utils.openai_management import process_env, submit_batch_file
from kcatextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from kcatextract.utils.working import pmid_to_tables_from
from kcatextract.utils.yaml_process import get_pmid_to_yaml_dict
from kcatextract.utils.openai_schema import to_openai_batch_request_with_schema
from kcatextract.utils.xml_pipeline import xml_get_soup, xml_abstract_processing, xml_raw_text_processing, xml_table_processing
from kcatextract.utils.xml_cals import parse_cals_table

def process_xml(filepath, original_tables=True):
    """
    original_tables: if True, will give tables as original xml.
    If False, will give tables as an abridged version
    If None: will skip tables
    """
    with open(filepath, "r", encoding='utf-8') as f:
        soup = xml_get_soup(f.read())

    # see EnzyXMLDocumentLoader.lazy_load
    # TODO search for xocs:ucs-locator
    if not soup:
        # print("Could not parse", filepath)
        return None

    # assume that the abstract fits into one chunk ;-;
    abstract = xml_abstract_processing(soup)
    
    # extract raw texts
    raw_txt = xml_raw_text_processing(soup)
    
    docs = []
    if original_tables:
        tables = soup.find_all('ce:table')
        # give each table directly, as raw xml
        for table in tables:
            raw_table = str(table)
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/dtd"', '')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/cals/dtd"', '')
            if raw_table.strip():
                docs.append(raw_table)
# raw_table = f"""
# Tables: ```xml
# {raw_table}
# ```
# """
    elif original_tables is False:
        # use old table extraction method
        for raw_table in xml_table_processing(soup):
            docs.append(raw_table)
    elif original_tables is None:
        pass
    
    docs.append(f"Abstract: {abstract}")
    docs.append(f"Text: {raw_txt}")
    
    # if not alt_re.search(combined):
    #     if not any(kcat_re.search(combined) for kcat_re in kcat_re_s):
    #         continue
    # convert docs to langchain docs
    return docs


def get_pure_tables(filepath, original_tables=True):
    """
    only extract the xml tables from the filepath
    """
    from lxml import etree
    import io


    with open(filepath, "r", encoding='utf-8') as f:
        # Parse the XML directly with lxml
        tree = etree.parse(f)

    tables = []
    # dfs = []
    if original_tables:
        # Use XPath to find all `ce:table` elements
        namespaces = {
            'ce': 'http://www.elsevier.com/xml/common/dtd',  # Update with the actual namespace if different
        }
        table_elements = tree.xpath('//ce:table', namespaces=namespaces)
        
        for table in table_elements:
            # Convert each table element back to a string without unnecessary namespaces
            raw_table = etree.tostring(table, encoding='unicode')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/dtd"', '')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/cals/dtd"', '')
            
            if raw_table.strip():  # Avoid empty entries
                tables.append(raw_table)
            
            # see if pandas can parse it
            # try:
            #     df = pd.read_xml(io.StringIO(raw_table))
            #     if df is not None:
            #         dfs.append(df)
            # except Exception as e:
            #     print("Error parsing table")
            #     print(e)

    return tables

process_env('.env')

namespace = 'openelse-brenda-cobble-t4neboth' # 'wos-open-apogee-429d-t2neboth'

# defaults
dest_folder = 'batches/enzy/bucket'
prompt = prompt_collections.table_oneshot_v3

xml_root = "c:/conjunct/vandy/yang/dois/elsevier/downloads"

structured = False
if namespace.endswith('-mini'):
    
    model_name = 'gpt-4o-mini' # 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
elif namespace.endswith('-tuned'):
    
    prompt = prompt_collections.table_oneshot_v1
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
elif namespace.endswith('-tuneboth'):
        
    prompt = prompt_collections.table_oneshot_v1_2
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:readboth:9wwLXS4i' # gpt-4o
elif namespace.endswith('-t2neboth'):
            
    prompt = prompt_collections.table_oneshot_v3
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t2neboth:9zuhXZVV' # gpt-4o
elif namespace.endswith('-t3neboth'):
    prompt = prompt_collections.table_oneshot_v3
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t3neboth:AOpwZY6M'
elif namespace.endswith('-t4neboth'):
    prompt = prompt_collections.table_oneshot_v3
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:t4neboth:AQOYyPCz'

elif namespace.endswith('-oneshot') or namespace.endswith('-4o'):
        
    # prompt = prompt_collections.table_oneshot_v1
    model_name = 'gpt-4o-2024-05-13'
elif namespace.endswith('-4os'):
    model_name = 'gpt-4o-2024-08-06' 
elif namespace.endswith('-4o-str'): # structured output
    model_name = 'gpt-4o-2024-08-06' 
    structured = True
    
else:
    raise ValueError("Unrecognized namespace", namespace)

batch = []

# setup
version = next_available_version(dest_folder, namespace, '.jsonl')
print("Namespace: ", namespace)
print("Using version: ", version)


# only use certain pmids
# _whitelist_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv')
# acceptable_pmids = set([str(int(x)) for x in _whitelist_df['pmid']])
acceptable_pmids = pmids_from_directory(xml_root, filetype='.xml')

# whitelist = pmids_from_cache("apogee_429")
# blacklist_df = pd.read_csv('data/mbrenda/_cache_openelse-brenda-xml-4o_1.csv', dtype={'pmid': str})
# blacklist = set(blacklist_df['pmid'])
whitelist = pmids_from_cache('brenda')

blacklist = set() # pmids_from_cache("brenda")

# disallowed_pmids = pmids_from_cache("brenda_rekcat_pdfs")


# target_pmids = acceptable_pmids #  - disallowed_pmids
# target_pmids = acceptable_pmids & pmid_to_tables.keys()
if 'whitelist' not in locals():
    whitelist = acceptable_pmids
target_pmids = (acceptable_pmids & whitelist) - blacklist # - disallowed_pmids
# target_pmids = acceptable_pmids & whitelist - disallowed_pmids

print(f"Using pmids {len(acceptable_pmids)} -> {len(target_pmids)}")

all_xml_tables = {}
all_dfs = []
for filepath in tqdm(glob.glob(f"{xml_root}/*.xml")):
    filename = os.path.basename(filepath)
    pmid = filename.rsplit('.', 1)[0]
    
    if pmid not in target_pmids:
        continue


    # all_xml_tables[filename] = tables
    # look at tables
    # temp thing to look at tables

    
    try:
        # doc = pymupdf.open(filepath)
        docs = process_xml(filepath, original_tables=None)
        if not docs:
            continue
    except Exception as e:
        print("Error opening", filepath)
        print(e)
        continue
    
    tables = get_pure_tables(filepath)
    table_docs = []
    for table in tables:
        html, text = parse_cals_table(table)
        content = ''
        if text:
            content = text + '\n\n'
            

        if html is not None:
            try:
                df = pd.read_html(io.StringIO(html))
                if df:
                    df = df[0]
                    content += df.fillna('').to_markdown() + '\n\n'
            except Exception as e:
                print(e)
        table_docs.append(content)
    docs = table_docs + docs
    
    
    # now make a batch
    if structured:
        req = to_openai_batch_request_with_schema(f'{namespace}_{version}_{pmid}', prompt, docs,
                                                    model_name=model_name)
    else:
        req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, docs, 
                                  model_name=model_name)
    batch.append(req)

# temp thing to look at tables
# with open('data/dev/all_xml_tables.json', 'w') as f:
    # json.dump(all_xml_tables, f)
# exit(0)

print("Using model", model_name)

    
will_write_to = f'{dest_folder}/{namespace}_{version}.jsonl'
# write in chunks

chunk_size = 1000
have_multiple = len(batch) > chunk_size # need to enforce chunk size, since OpenAI has data size limit
for i in range(0, len(batch), chunk_size):
    chunk = batch[i:i+chunk_size]
    if have_multiple:
        will_write_to = f'{dest_folder}/{namespace}_{version}.{i}.jsonl'
    write_to_jsonl(chunk, will_write_to)


    try:
        batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", will_write_to)
        print(e)

    # with open('batches/pending.jsonl', 'a') as f:
    #     f.write(json.dumps({'input': f'{namespace}_{version}', 'output': batchname}))
    #     f.write('\n')
