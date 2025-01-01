# this uses varinvar_explode to explode the descriptors

import json
import re
import pandas as pd
import pymupdf
import glob
import os
from tqdm import tqdm

from enzyextract.backform.backform_utils import fix_the_yaml, isolate_the_yaml
from enzyextract.explode.explode_auto_context import create_explode_batch, pmids_needing_exploding
from enzyextract.submit.batch_utils import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.submit.openai_management import process_env, submit_batch_file
from enzyextract.utils.pmid_management import pmids_from_cache
from enzyextract.utils import prompt_collections
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.yaml_process import get_pmid_to_yaml_dict


batch = []

# setup
dest_folder = 'batches/explode'
# orig_namespace = 'oneshot-tuned'
orig_namespace = 'brenda-rekcat-tuneboth'


# root = 'C:/conjunct/table_eval/completions/enzy'
root = 'completions/enzy'
at, og_version = locate_correct_batch(root, orig_namespace)

namespace = f'explode-for-{orig_namespace}-{og_version}'
# namespace = 'explode-for-tabled-oneshot-tuned'
# orig_namespace = namespace.split('explode-for-', 1)[1]


version = next_available_version(dest_folder, namespace, '.jsonl')
# version = 1
print("Using version:", version)

# whitelist = pmids_from_cache('explode/tabled-oneshot-tuned')


# def script0():
#     for custom_id, content, finish_reason in get_batch_output(f'{root}/{at}'):
#         pmid = str(pmid_from_usual_cid(custom_id))
#         # if pmid not in whitelist:
#         #     continue
#         if finish_reason == 'length':
#             print("Too long:", pmid)
#             continue
        
#         content = re.sub('\bextras: ?\n', 'data:\n', content) # blunder
#         def fix_my_yaml(yaml_block):
#             fixed_yaml = ""
#             mode = None
#             for line in yaml_block.split("\n"):
#                 if line.startswith("data:"):
#                     mode = "data"
#                 elif line.startswith("context:"):
#                     mode = "context"
#                 if mode == "context" or line.startswith("data:") or line.startswith("    - descriptor:"):
#                     fixed_yaml += line + "\n" # only keep specific lines
#             return fixed_yaml
        
#         gpt_input = fix_the_yaml(content, fix_my_yaml)
#         docs = [gpt_input]

#         req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt_collections.explode_1v1, docs, 
#                                     model_name='gpt-4o')
#         batch.append(req)

path_to_matched_df = f'data/valid/_valid_{orig_namespace}_{og_version}.csv'
df = pd.read_csv(path_to_matched_df)
pmids = pmids_needing_exploding(df)
batch = create_explode_batch(pmids, namespace, version, f'{root}/{at}', prompt=prompt_collections.explode_1v3)

print("Wrote", len(batch))
sent_to = f'{dest_folder}/{namespace}_{version}.jsonl'
write_to_jsonl(batch, sent_to)

# preview
# print(batch[0]['prompt'])
print(batch[0]['body']['messages'][-1]['content'])

process_env('.env')
batchname = submit_batch_file(sent_to) # will ask for confirmation

with open('batches/pending.jsonl', 'a') as f:
    f.write(json.dumps({'input': f'{namespace}_{version}', 'output': batchname}) + '\n')