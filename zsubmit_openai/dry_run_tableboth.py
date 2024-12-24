# working_enzy_table_md, but tableless

import json
import re
import polars as pl
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from enzyextract.utils import prompt_collections
from enzyextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.micro_fix import duplex_mM_corrected_text
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema
from enzyextract.utils.namespace_management import glean_model_name

namespace = 'dry-run-t2neboth' # 'brenda-pnas-apogee-4o-str' # 'wos-open-apogee-429d-t2neboth'

# defaults
prompt = prompt_collections.table_oneshot_v3 # 1_2 # doesn't matter
# md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'


# tables_from = r'C:\conjunct\tmp\eval\beluga_dev\tables'
tables_from = r"C:\conjunct\tmp\eval\manifold_tune\tables"
pdf_root = 'C:/conjunct/tmp/eval/manifold_tune/pdfs'
# micro_path = "C:/conjunct/tmp/eval/beluga_dev/mM.csv"
micro_path = "C:/conjunct/tmp/eval/cherry_dev/mM/mMcurated.parquet"
# micro_path = 'zpreprocessing/data/pdf_mM.parquet'

# keep pages separate, or not?
keep_pages = False
# keep_pages = False


# will_write_to = f'_debug/dry_run.jsonl'
will_write_to = f'zfinetune/inputs/manifold_tune.jsonl'



model_name, suggested_prompt, structured = glean_model_name(namespace)
prompt = suggested_prompt if suggested_prompt else prompt

batch = []

# setup
version = 1
print("Namespace: ", namespace)
print("Using version: ", version)

pmid_to_tables = {}
if tables_from is not None:
    pmid_to_tables = pmid_to_tables_from(tables_from)
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
if micro_path.endswith('.parquet'):
    micro_df = pl.read_parquet(micro_path).to_pandas()
else:
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


if _num_in_micro == 0:
    # try removing '.pdf' from the pdfname
    micro_df['pdfname'] = micro_df['pdfname'].str.replace("\.pdf$", "", regex=True)

for pmid in target_pmids:
    if pmid in micro_df['pdfname'].values:
        _num_in_micro += 1
print(f"Intersection of {_num_in_micro} pmids with micro corrections")
assert _num_in_micro > 0, "No intersection of micro corrections found"

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
    
    
    if pmid_to_tables and pmid in pmid_to_tables:
        for filename in pmid_to_tables.get(pmid, []):
            with open(f'{tables_from}/{filename}', 'r', encoding='utf-8') as f:
                docs.append(f.read())
        _pmid_with_tables += 1
    
    # 
    # for page in doc:
        # docs.append(page.get_text())
    # best micro re
    widest_mM_re = re.compile(r'\bmm(?=$|[\Wo2])', re.IGNORECASE)
    # \u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0011\u0012\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f
    ascii_control_re = re.compile(r'(?<!\w)[\x00-\x08\x11\x12\x14-\x1F]M\b') # \x7F-\x9F
    pages = duplex_mM_corrected_text(doc, pmid, micro_df, _re=widest_mM_re)
    # post-processing
    for i, page in enumerate(pages):
        if 'µMo' in page:
            pass
            # print("Warning: funny looking capitalization issue in", pmid)
        txt = page.replace('µMo', 'µmo') # fix funny looking capitalization issue in post
        txt = ascii_control_re.sub('µM', txt)
        pages[i] = txt
    docs.extend(pages)
    

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

    

# get content 
prompts = [(x['body']['messages'][0]['content']) for x in batch]
if keep_pages:
    contents = [] # list of lists of strings
    for x in batch:
        builder = []
        for y in x['body']['messages'][1:]:
            builder.append(y['content'])
        contents.append(builder)
else:
    contents = ['\n'.join(y['content'] for y in x['body']['messages'][1:]) for x in batch] # list of strings
# pagess = [(y['content'] for y in x['body']['messages'][1:]) for x in batch]

if keep_pages:
    so = {
        'content': pl.List(pl.Utf8),
    }
else:
    so = {
        'content': pl.Utf8,
    }
df = pl.DataFrame({'custom_id': [x['custom_id'] for x in batch], 
                   'pmid': [x['custom_id'].split('_', 2)[2] for x in batch], 
                   'content': contents,
                    # 'pages': pagess,
                   'prompt': prompts}, schema_overrides=so)
df.write_parquet(will_write_to + '.parquet')
with open(will_write_to, 'w') as f:
    for x in batch:
        f.write(json.dumps(x) + '\n')
# write in chunks
