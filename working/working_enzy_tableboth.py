# working_enzy_table_md, but tableless

import json
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from kcatextract.utils import prompt_collections
from kcatextract.utils.construct_batch import get_pmid_to_yaml_dict, to_openai_batch_request, write_to_jsonl
from kcatextract.utils.fresh_version import next_available_version
from kcatextract.utils.micro_fix import mM_corrected_text
from kcatextract.utils.pmid_management import pmids_from_batch, pmids_from_cache, pmids_from_directory
from kcatextract.utils.working import pmid_to_tables_from


# def obtain_yamls(file_path):
#     result = {}
#     with open(file_path, 'r', encoding='utf-8') as f:
#         for yaml, pmid in extract_yaml_code_blocks(f.read()):
#             if pmid not in result:
#                 result[pmid] = ""
#             result[pmid] += yaml + '\n'
#     return result

namespace = 'brenwi-giveboth-tuned'

# defaults
micro_path = "C:/conjunct/vandy/yang/reocr/results/micros_resnet_v1.csv"
dest_folder = 'batches/enzy'
# md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'

table_info_root = None
table_md_src = None
md_folder = None
if namespace.startswith('tableless-') or namespace.startswith('tabled-') or namespace.startswith('rekcat-'):
    pdf_root = "C:/conjunct/tmp/brenda_rekcat_pdfs"
    # table_info_root = "C:/conjunct/tmp/brenda_rekcat_tables"
    # table_md_src = "completions/enzy/brenda-rekcat-md-v1-2_1.md"
    
elif namespace.startswith('brenwi-'):
    pdf_root = "D:/brenda/wiley"
    md_folder = "C:/conjunct/vandy/yang/corpora/tabular/brenda/wiley_v6"
    micro_path = "C:/conjunct/vandy/yang/reocr/results/micros_brenda_wiley_v1.csv"
elif namespace.startswith('brenjbc-'):
    pdf_root = "D:/brenda/jbc"
    micro_path = "C:/conjunct/vandy/yang/reocr/results/brenda/micros_brenda_jbc.csv"
elif namespace.startswith('brenasm-'):
    pdf_root = "D:/brenda/asm"
    micro_path = "C:/conjunct/vandy/yang/reocr/results/brenda/micros_brenda_asm.csv"


else:
    raise ValueError("Unrecognized prefix", namespace)
    
# if "-giveboth-" in namespace:
#     md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
#     prompt = prompt_collections.table_oneshot_v1_1
    
if namespace.endswith('-mini'):
    
    # prompt = prompt_collections.table_oneshot_v2 # v1
    model_name = 'gpt-4o-mini' # 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
elif namespace.endswith('-tuned'):
    
    prompt = prompt_collections.table_oneshot_v1
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
elif namespace.endswith('-tuneboth'):
        
    prompt = prompt_collections.table_oneshot_v1_2
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:readboth:9wwLXS4i' # gpt-4o
elif namespace.endswith('-oneshot') or namespace.endswith('-4o'):
        
    # prompt = prompt_collections.table_oneshot_v1
    model_name = 'gpt-4o'
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

pmid_to_tables = None
if md_folder is not None:
    pmid_to_tables = pmid_to_tables_from(md_folder)
    assert pmid_to_tables, "No tables found"

# only use certain pmids
# _whitelist_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_63.csv')
# acceptable_pmids = set([str(int(x)) for x in _whitelist_df['pmid']])
acceptable_pmids = pmids_from_directory(pdf_root)

# # disallowed_pmids = pmids_from_cache("brenda_rekcat_pdfs")
# target_pmids = acceptable_pmids #  - disallowed_pmids
target_pmids = acceptable_pmids
# # target_pmids = pmids_from_batch(f'batches/enzy/brenda-rekcat-md-v1-2_1.jsonl')
# print(f"Using pmids {len(acceptable_pmids)} -> {len(target_pmids)}")

REDACT = False

# apply micro fix
micro_df = pd.read_csv(micro_path)
micro_df = micro_df.astype({'pdfname': 'str'})
_pmid_with_tables = 0
for filepath in tqdm(glob.glob(f"{pdf_root}/*.pdf")):
    # print(filename)
    filename = os.path.basename(filepath)
    pmid = filename.rsplit('.', 1)[0]
    
    if pmid not in target_pmids:
        continue
    
    doc = pymupdf.open(filepath)
    
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
    req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, docs, 
                                  model_name=model_name)
    batch.append(req)

write_to_jsonl(batch, f'{dest_folder}/{namespace}_{version}.jsonl')
if _pmid_with_tables:
    print(f"Found {_pmid_with_tables} pmids with tables")