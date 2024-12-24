# working_enzy_table_md, but tableless

import json
import polars as pl
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from enzyextract.utils import prompt_collections
from enzyextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.micro_fix import duplex_mM_corrected_text
from enzyextract.utils.openai_management import process_env, submit_batch_file
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.utils.yaml_process import get_pmid_to_yaml_dict
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema

namespace = 'beluga-t2neboth' # 'brenda-pnas-apogee-4o-str' # 'wos-open-apogee-429d-t2neboth'

# defaults
dest_folder = 'batches/enzy'
prompt = prompt_collections.table_oneshot_v3 # 1_2
# md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'



table_info_root = None
table_md_src = None
md_folder = None
pdf_root = 'C:/conjunct/tmp/eval/arctic'
micro_path = "C:/conjunct/tmp/eval/beluga_dev/mM.csv"


from enzyextract.utils.namespace_management import glean_model_name
model_name, suggested_prompt, structured = glean_model_name(namespace)

prompt = suggested_prompt if suggested_prompt else prompt

batch = []

# setup
version = next_available_version(dest_folder, namespace, '.jsonl')
print("Namespace: ", namespace)
print("Using version: ", version)

pmid_to_yaml = {}
if table_md_src is not None:
    pmid_to_yaml = get_pmid_to_yaml_dict(table_md_src)

pmid_to_tables = {}
if md_folder is not None:
    pmid_to_tables = pmid_to_tables_from(md_folder)
    assert pmid_to_tables, "No tables found"



# only use certain pmids
# _whitelist_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv')
# acceptable_pmids = set([str(int(x)) for x in _whitelist_df['pmid']])
acceptable_pmids = pmids_from_directory(pdf_root)

# whitelist = pmids_from_cache("apogee_429")

disallowed_pmids = set() # pmids_from_cache("brenda_rekcat_pdfs")


# target_pmids = acceptable_pmids #  - disallowed_pmids
# target_pmids = acceptable_pmids & pmid_to_tables.keys()
target_pmids = acceptable_pmids - disallowed_pmids
# target_pmids = acceptable_pmids & whitelist - disallowed_pmids


# fix oopsie: only look at pmids that start with "10." and are in pmid_to_tables
# target_pmids = {pmid for pmid in target_pmids if pmid.startswith("10.") and pmid in pmid_to_tables}

# # target_pmids = pmids_from_batch(f'batches/enzy/brenda-rekcat-md-v1-2_1.jsonl')
print(f"Using pmids {len(acceptable_pmids)} -> {len(target_pmids)}")

# make sure this is an intersection between what we will read and pmid_to_tables
_intersect = 0
for pmid in target_pmids:
    if pmid in pmid_to_tables:
        _intersect += 1
print(f"Intersection of {_intersect} pmids with tables")
assert _intersect >= 0, "No intersection of tables found"

REDACT = False

# apply micro fix
micro_df = pd.read_csv(micro_path)
micro_df = micro_df.astype({'pdfname': 'str'})

# only want 
true_micro_df = micro_df[(micro_df['real_char'] == "mu") & (micro_df['confidence'] > 0.98)]
# micro_df = true_micro_df
true_m_df = micro_df[micro_df['real_char'] == "m"]
micro_df = pd.concat([true_micro_df, true_m_df], ignore_index=True)


# sanity check: ensure that some of the pmids are in the micro_df
_num_in_micro = 0
for pmid in target_pmids:
    if pmid in micro_df['pdfname'].values:
        _num_in_micro += 1
print(f"Intersection of {_num_in_micro} pmids with micro corrections")
assert _num_in_micro >= 0, "No intersection of micro corrections found"

_pmid_with_tables = 0
for filepath in tqdm(glob.glob(f"{pdf_root}/*.pdf")):
    # print(filename)
    filename = os.path.basename(filepath)
    pmid = filename.rsplit('.', 1)[0]
    
    if pmid not in target_pmids:
        continue
    
    try:
        doc = pymupdf.open(filepath)
    except Exception as e:
        print("Error opening", filepath)
        print(e)
        continue
    
    tables = {}
    for info in os.listdir(table_info_root):
        if info.startswith(pmid + '_') and info.endswith('.info'):
            obj = json.load(open(f"{table_info_root}/{info}", 'r'))
            if obj['page_no'] not in tables:
                tables[obj['page_no']] = []
            tables[obj['page_no']].append(obj)
    # now we have tables
    # actually exclude text 
    # use the redact function of pymupdf
    
    if REDACT:
        pass
        # for page_no, table in tables.items():
        #     page = doc[page_no]
        #     for obj in table:
        #         rect = obj['bbox']
        #         # https://github.com/pymupdf/PyMuPDF/issues/698
        #         annot = page.add_redact_annot(rect)
        #         page.apply_redactions(images=pymupdf.PDF_REDACT_IMAGE_NONE)
            # do NOT save
    
    # now obtain texts
    docs = []
    
    # provide original yaml
    if pmid in pmid_to_yaml:
        # continue # skip those with tables. Expect N=1078
        # docs.append(pmid_to_yaml[pmid])
        continue
    else:
        pass
        # no yaml available
    
    if pmid_to_tables and pmid in pmid_to_tables:
        for filename in pmid_to_tables.get(pmid, []):
            with open(f'{md_folder}/{filename}', 'r', encoding='utf-8') as f:
                docs.append(f.read())
        _pmid_with_tables += 1
    
    # 
    # for page in doc:
        # docs.append(page.get_text())
    docs.extend(duplex_mM_corrected_text(doc, pmid, micro_df))
    

    # obtain original annotation from part A
    # use the table_md_root


    # now make a batch
    # if len(docs) < 2:
    #     print("Warning: not enough data for", pmid)
    #     continue
    if structured:
        req = to_openai_batch_request_with_schema(f'{namespace}_{version}_{pmid}', prompt, docs,
                                                    model_name=model_name)
    else:
        req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, docs, 
                                  model_name=model_name)
    batch.append(req)

if _pmid_with_tables:
    print(f"Found {_pmid_with_tables} pmids with tables")
else:
    print("WARNING: no pmids with tables")
print("Using model", model_name)

    
will_write_to = f'_debug/dry_run.jsonl'

# get content 
contents = [''.join(y['content'] for y in x['body']['messages']) for x in batch]
df = pl.DataFrame({'pmid': [x['custom_id'] for x in batch], 'content': contents})
df.write_parquet(will_write_to + '.parquet')
# write in chunks
