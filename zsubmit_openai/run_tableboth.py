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
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.micro_fix import duplex_mM_corrected_text
from enzyextract.utils.openai_management import process_env, submit_batch_file
from enzyextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from enzyextract.utils.working import pmid_to_tables_from
from enzyextract.utils.yaml_process import get_pmid_to_yaml_dict
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema


process_env('.env')

namespace = 'wos-localshim-t2neboth' # 'brenda-pnas-apogee-4o-str' # 'wos-open-apogee-429d-t2neboth'

# defaults
# micro_path = "C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv"
micro_path = 'zpreprocessing/data/pdf_mM.parquet'
dest_folder = 'batches/enzy'
prompt = prompt_collections.table_oneshot_v3 # 1_2
# md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'



table_info_root = None
table_md_src = None
tables_from = None

if namespace.startswith('tableless-') or namespace.startswith('tabled-') \
        or namespace.startswith('rekcat-') or namespace.startswith('brenda-rekcat-'):
    pdf_root = "C:/conjunct/tmp/brenda_rekcat_pdfs"
    tables_from = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
    # table_info_root = "C:/conjunct/tmp/brenda_rekcat_tables"
    # table_md_src = "completions/enzy/brenda-rekcat-md-v1-2_1.md"
elif namespace.startswith('arctic-'):
    pdf_root = 'C:/conjunct/tmp/eval/arctic'
    if namespace.startswith('arctic-nomu-'):
        # throwaway
        micro_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_topoff_hindawi.csv"
        tables_from = 'C:/conjunct/tmp/eval/arctic_dev/tables_no_mu'
    elif namespace.startswith('arctic-control-'):
        micro_path = 'C:/conjunct/tmp/eval/arctic_dev/mM.csv'
        tables_from = None
    else:
        micro_path = 'C:/conjunct/tmp/eval/arctic_dev/mM.csv'
        tables_from = 'C:/conjunct/tmp/eval/arctic_dev/tables'
elif namespace.startswith('beluga-'):
    pdf_root = 'C:/conjunct/tmp/eval/arctic'
    micro_path = 'C:/conjunct/tmp/eval/beluga_dev/mM.csv'
    tables_from = 'C:/conjunct/tmp/eval/beluga_dev/tables'
elif namespace.startswith('cherry-dev-'):
    pdf_root = 'C:/conjunct/tmp/eval/arctic'
    micro_path = 'zpreprocessing/data/pdf_mM.parquet'
    tables_from = 'C:/conjunct/tmp/eval/cherry_dev/tables'
else:
    toplevels = ['brenda', 'scratch', 'wos', 'topoff']
    secondlevels = ['wiley', 'open', 'open1', 'open2', 'open3', 'asm', 'jbc', 'hindawi', 'openremote', 'local_shim', 'localshim', 'pnas', 'scihub']
    found = False
    for top in toplevels:
        if namespace.startswith(f"{top}-"):
            for second in secondlevels:
                if namespace.startswith(f'{top}-{second}-'):
                    pdf_root = f"D:/papers/{top}/{second}"
                    if second in ['open1', 'open2', 'open3']:
                        pdf_root = f"D:/papers/{top}/open-part{second[-1]}"
                        second = 'open'

                        dest_folder = 'batches/enzy/apatch'
                    elif second in 'localshim':
                        pdf_root = f"D:/papers/{top}/local_shim"
                        second = 'local_shim'
                        dest_folder = 'batches/enzy/apatch'
                    if os.path.exists(f"C:/conjunct/tmp/eval/cherry_prod/tables/{top}/{second}"):
                        print("Using cherry tables and mM")
                        tables_from = f"C:/conjunct/tmp/eval/cherry_prod/tables/{top}/{second}"
                    else:
                        tables_from = f"C:/conjunct/vandy/yang/corpora/tabular/{top}/{second}"
                        micro_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_{top}_{second}.csv"
                    found = True
                    break
            if found:
                break
    if not found:
        raise ValueError("Unrecognized prefix", namespace)

from enzyextract.utils.namespace_management import glean_model_name
model_name, suggested_prompt, structured = glean_model_name(namespace)

prompt = suggested_prompt if suggested_prompt else prompt

batch = []

# setup
version = next_available_version(dest_folder, namespace, '.jsonl')
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



target_pmids = acceptable_pmids - disallowed_pmids

manifest = pl.read_parquet('data/manifest.parquet')
# only readable
manifest = manifest.with_columns([
    pl.col('filename').str.replace('\.pdf$', '').alias('pmid')
])
manifest_view = manifest.filter(
    pl.col('readable')
    & ~pl.col('bad_ocr')
    & pl.col('pmid').is_in(target_pmids)
).unique('filename').select(['fileroot', 'filename', 'pmid'])


# fix oopsie: only look at pmids that start with "10." and are in pmid_to_tables
# target_pmids = {pmid for pmid in target_pmids if pmid.startswith("10.") and pmid in pmid_to_tables}

# # target_pmids = pmids_from_batch(f'batches/enzy/brenda-rekcat-md-v1-2_1.jsonl')
print(f"Using pmids {len(acceptable_pmids)} -> {manifest_view.height}")
# target_pmids = set(manifest_view['pmid'])

# make sure this is an intersection between what we will read and pmid_to_tables
_intersect = 0
for pmid in manifest_view['pmid']:
    if pmid in pmid_to_tables:
        _intersect += 1
print(f"Intersection of {_intersect} pmids with tables")
assert _intersect >= 0, "No intersection of tables found"

REDACT = False

# apply micro fix
if micro_path.endswith('.parquet'):
    micro_df = pl.read_parquet(micro_path) # .to_pandas()
else:
    micro_df = pd.read_csv(micro_path)
    micro_df = micro_df.astype({'pdfname': 'str'})

# only want 
# true_micro_df = micro_df[(micro_df['real_char'] == "mu") & (micro_df['confidence'] > 0.98)]
true_micro_df = micro_df.filter(
    (pl.col('real_char') == "mu") 
    & (pl.col('confidence') > 0.98)
)
# micro_df = true_micro_df
# true_m_df = micro_df[micro_df['real_char'] == "m"]
true_m_df = micro_df.filter(pl.col('real_char') == "m")

micro_df = pl.concat([true_micro_df, true_m_df]) # , ignore_index=True)


# sanity check: ensure that some of the pmids are in the micro_df
_num_in_micro = len(set(micro_df['pdfname']).intersection(set(manifest_view['pmid'])))
if _num_in_micro == 0:
    # try removing '.pdf' from the pdfname
    micro_df = micro_df.with_columns([
        (pl.col('pdfname').str.replace("\.pdf$", "")).alias('pdfname')
    ])
    _num_in_micro = len(set(micro_df['pdfname']).intersection(set(target_pmids)))
print(f"Intersection of {_num_in_micro} pmids with micro corrections")
assert _num_in_micro > 0, "No intersection of micro corrections found"

_pmid_with_tables = 0
# for fileroot in tqdm(glob.glob(f"{pdf_root}/*.pdf")):
for fileroot, filename, pmid in tqdm(manifest_view.iter_rows(), total=manifest_view.height):
    # print(filename)
    # filename = os.path.basename(fileroot)
    # pmid = filename.rsplit('.', 1)[0]
    
    # if pmid not in target_pmids:
    #     continue
    assert pmid in target_pmids
    
    try:
        doc = pymupdf.open(fileroot + '/' + filename)
    except Exception as e:
        print("Error opening", fileroot)
        print(e)
        continue
    
    if len(doc) > 100:
        continue


    
    if False: # REDACT:
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
        for page_no, table in tables.items():
            page = doc[page_no]
            for obj in table:
                rect = obj['bbox']
                # https://github.com/pymupdf/PyMuPDF/issues/698
                annot = page.add_redact_annot(rect)
                page.apply_redactions(images=pymupdf.PDF_REDACT_IMAGE_NONE)
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
        # if 'µMo' in page:
            # print("Warning: funny looking capitalization issue in", pmid)
            # pass
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
        if i == 0:
            # write the first one as an inspection, for debugging purposes

            # get content 
            prompts = [(x['body']['messages'][0]['content']) for x in chunk]
            contents = ['\n'.join(y['content'] for y in x['body']['messages'][1:]) for x in chunk] # list of strings
            df = pl.DataFrame({'custom_id': [x['custom_id'] for x in chunk], 
                    'pmid': [x['custom_id'].split('_', 2)[2] for x in chunk], 
                    'content': contents,
                        # 'pages': pagess,
                    'prompt': prompts})
            df.write_parquet('_debug/latest_tableboth.parquet')
    
        batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", will_write_to)
        print(e)

    # with open('batches/pending.jsonl', 'a') as f:
    #     f.write(json.dumps({'input': f'{namespace}_{version}', 'output': batchname}))
    #     f.write('\n')