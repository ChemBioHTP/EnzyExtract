# working_enzy_table_md, but tableless

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


# def obtain_yamls(file_path):
#     result = {}
#     with open(file_path, 'r', encoding='utf-8') as f:
#         for yaml, pmid in extract_yaml_code_blocks(f.read()):
#             if pmid not in result:
#                 result[pmid] = ""
#             result[pmid] += yaml + '\n'
#     return result
process_env('.env')

namespace = 'beluga-t2neboth' # 'brenda-pnas-apogee-4o-str' # 'wos-open-apogee-429d-t2neboth'

# defaults
micro_path = "C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv"
dest_folder = 'batches/enzy'
prompt = prompt_collections.table_oneshot_v3 # 1_2
# md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'



table_info_root = None
table_md_src = None
md_folder = None

if namespace.startswith('tableless-') or namespace.startswith('tabled-') \
        or namespace.startswith('rekcat-') or namespace.startswith('brenda-rekcat-'):
    pdf_root = "C:/conjunct/tmp/brenda_rekcat_pdfs"
    md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
    # table_info_root = "C:/conjunct/tmp/brenda_rekcat_tables"
    # table_md_src = "completions/enzy/brenda-rekcat-md-v1-2_1.md"
elif namespace.startswith('arctic-'):
    pdf_root = 'C:/conjunct/tmp/eval/arctic'
    if namespace.startswith('arctic-nomu-'):
        # throwaway
        micro_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_topoff_hindawi.csv"
        md_folder = 'C:/conjunct/tmp/eval/arctic_dev/tables_no_mu'
    elif namespace.startswith('arctic-control-'):
        micro_path = 'C:/conjunct/tmp/eval/arctic_dev/mM.csv'
        md_folder = None
    else:
        micro_path = 'C:/conjunct/tmp/eval/arctic_dev/mM.csv'
        md_folder = 'C:/conjunct/tmp/eval/arctic_dev/tables'
elif namespace.startswith('beluga-'):
    pdf_root = 'C:/conjunct/tmp/eval/arctic'
    micro_path = 'C:/conjunct/tmp/eval/beluga_dev/mM.csv'
    md_folder = 'C:/conjunct/tmp/eval/beluga_dev/tables'
else:
    toplevels = ['brenda', 'scratch', 'wos', 'topoff']
    secondlevels = ['wiley', 'open', 'asm', 'jbc', 'hindawi', 'openremote', 'local_shim', 'pnas', 'scihub']
    found = False
    for top in toplevels:
        if namespace.startswith(f"{top}-"):
            for second in secondlevels:
                if namespace.startswith(f'{top}-{second}-'):
                    pdf_root = f"D:/{top}/{second}"
                    md_folder = f"C:/conjunct/vandy/yang/corpora/tabular/{top}/{second}"
                    micro_path = f"C:/conjunct/vandy/yang/reocr/cache/iter3/mM_{top}_{second}.csv"
                    found = True
                    break
            if found:
                break
    if not found:
        raise ValueError("Unrecognized prefix", namespace)
            
# elif namespace.startswith('brenwi-') or namespace.startswith('brenda-wiley-'):
#     pdf_root = "D:/brenda/wiley"
#     # md_folder = "C:/conjunct/vandy/yang/corpora/tabular/brenda/wiley_v6"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/micros_brenda_wiley_v1.csv"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/brenda/wiley"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_brenda_wiley.csv"
# elif namespace.startswith('brenjbc-') or namespace.startswith('brenda-jbc-'):
#     pdf_root = "D:/brenda/jbc"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/brenda/micros_brenda_jbc.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_brenda_jbc.csv"
# elif namespace.startswith('brenasm-') or namespace.startswith('brenda-asm-'):
#     pdf_root = "D:/brenda/asm"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/brenda/micros_brenda_asm.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_brenda_asm.csv"
# elif namespace.startswith('scratch-wiley-'):
#     pdf_root = "D:/scratch/wiley"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/scratch/wiley_v4"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/scratch/micros_scratch_wiley.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_scratch_wiley.csv"
# elif namespace.startswith('scratch-open-'):
#     pdf_root = "D:/scratch/open"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/scratch/open_v6"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/scratch/micros_scratch_open.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_scratch_open.csv"
# elif namespace.startswith('scratch-asm-'):
#     pdf_root = "D:/scratch/asm"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/scratch/asm"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/scratch/micros_scratch_asm.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_scratch_asm.csv"
# elif namespace.startswith('scratch-open_remote-'):
#     pdf_root = "D:/scratch/open_remote"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/scratch/open_remote"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/scratch/micros_scratch_open_remote.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_scratch_open_remote.csv"
# elif namespace.startswith('scratch-hindawi-'):
#     pdf_root = "D:/scratch/hindawi"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/scratch/hindawi"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/scratch/micros_scratch_hindawi.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_scratch_hindawi.csv"
# elif namespace.startswith('wos-wiley-'):
#     pdf_root = "D:/wos/wiley"
#     md_folder = "C:/conjunct/vandy/yang/corpora/tabular/wos/wiley"
#     # micro_path = "C:/conjunct/vandy/yang/reocr/results/wos/micros_wos_wiley.csv"
#     micro_path = "C:/conjunct/vandy/yang/reocr/cache/iter3/mM_wos_wiley.csv"

# else:
#     raise ValueError("Unrecognized prefix", namespace)
    
# if "-giveboth-" in namespace:
#     md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
#     prompt = prompt_collections.table_oneshot_v1_1

structured = False
if namespace.endswith('-mini'):
    
    # prompt = prompt_collections.table_oneshot_v2 # v1
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
    docs.extend(mM_corrected_text(doc, pmid, micro_df))
    

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
        batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", will_write_to)
        print(e)

    # with open('batches/pending.jsonl', 'a') as f:
    #     f.write(json.dumps({'input': f'{namespace}_{version}', 'output': batchname}))
    #     f.write('\n')