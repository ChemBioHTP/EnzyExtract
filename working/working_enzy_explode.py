# this uses varinvar_explode to explode the descriptors

import json
import re
import pymupdf
import glob
import os
from tqdm import tqdm

from kcatextract.backform.backform_utils import fix_the_yaml, isolate_the_yaml
from kcatextract.utils.construct_batch import get_resultant_content, locate_correct_batch, pmid_from_usual_cid
from kcatextract.utils.pmid_management import pmids_from_cache
from kcatextract.utils import prompt_collections
from kcatextract.utils.construct_batch import get_pmid_to_yaml_dict, to_openai_batch_request, write_to_jsonl
from kcatextract.utils.fresh_version import next_available_version




pdf_root = "C:/conjunct/tmp/brenda_rekcat_pdfs"

table_info_root = "C:/conjunct/tmp/brenda_rekcat_tables"
table_md_src = "completions/enzy_eval/eval-brenda-gt_1.md"

batch = []

# setup
dest_folder = 'batches/enzy/explode'
namespace = 'explode-4o--tabled-oneshot-tuned'
orig_namespace = namespace.split('explode-4o--', 1)[1]

# version = next_available_version(dest_folder, namespace, '.jsonl')
version = 1
print("Using version:", version)

# pmid_to_yaml = get_pmid_to_yaml_dict(table_md_src)

whitelist = pmids_from_cache('explode/tabled-oneshot-tuned')

root = 'completions/enzy'
at, version = locate_correct_batch(root, orig_namespace)

for custom_id, content, finish_reason in get_resultant_content(f'{root}/{at}'):
    pmid = str(pmid_from_usual_cid(custom_id))
    if pmid not in whitelist:
        continue
    
    content = re.sub('\bextras: ?\n', 'data:\n', content) # blunder
    def fix_my_yaml(yaml_block):
        fixed_yaml = ""
        mode = None
        for line in yaml_block.split("\n"):
            if line.startswith("data:"):
                mode = "data"
            elif line.startswith("context:"):
                mode = "context"
            if mode == "context" or line.startswith("data:") or line.startswith("    - descriptor:"):
                fixed_yaml += line + "\n" # only keep specific lines
        return fixed_yaml
    
    gpt_input = fix_the_yaml(content, fix_my_yaml)
    docs = [gpt_input]

    req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt_collections.explode_1v0, docs, 
                                  model_name='gpt-4o')
    batch.append(req)

print("Wrote", len(batch))
write_to_jsonl(batch, f'{dest_folder}/{namespace}_{version}.jsonl')

# preview
# print(batch[0]['prompt'])
print(batch[0]['body']['messages'][-1]['content'])