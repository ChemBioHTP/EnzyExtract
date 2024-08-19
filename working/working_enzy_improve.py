# this uses part A of varinvar to improve the annotations with more stuff from the full text

import json
import pymupdf
import glob
import os
from tqdm import tqdm

from utils import prompt_collections
from utils.construct_batch import get_pmid_to_yaml_dict, to_openai_batch_request, write_to_jsonl
from utils.fresh_version import next_available_version



pdf_root = "C:/conjunct/tmp/brenda_rekcat_pdfs"

table_info_root = "C:/conjunct/tmp/brenda_rekcat_tables"
table_md_src = "completions/enzy_eval/eval-brenda-gt_1.md"

batch = []

# setup
dest_folder = 'batches/enzy/improve'
namespace = 'brenda-rekcat-partB'
version = next_available_version(dest_folder, namespace, '.jsonl')
print("Using version: ", version)

pmid_to_yaml = get_pmid_to_yaml_dict(table_md_src)

for filepath in tqdm(glob.glob(f"{pdf_root}/*.pdf")):
    # print(filename)
    filename = os.path.basename(filepath)
    pmid = filename.rsplit('.', 1)[0]
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
        docs.append(pmid_to_yaml[pmid])
    else:
        docs.append( # from scratch!
f"""\
No yaml available. Construct the output yaml directly.
```yaml
context:
    null
data:
    null
```                    
""")
    
    # 
    for page in doc:
        docs.append(page.get_text())
    

    # obtain original annotation from part A
    # use the table_md_root


    # now make a batch
    if len(docs) < 2:
        print("Warning: not enough data for", pmid)
        continue
    req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt_collections.table_varinvar_B_v1, docs, 
                                  model_name='gpt-4o-mini')
    batch.append(req)

write_to_jsonl(batch, f'{dest_folder}/{namespace}_{version}.jsonl')