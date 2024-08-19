import glob
import os

from kcatextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from kcatextract.utils.fresh_version import next_available_version
import kcatextract.utils.prompt_collections as prompt_collections
    
# setup
md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
dest_folder = 'batches/enzy'
# namespace = 'brenda-rekcat-md-v1-2'
namespace = 'tablemd-oneshot-tuned'
yaml_namespace = 'tabled-oneshot-tuned'



if '_' in namespace:
    raise ValueError("Namespace cannot contain underscores")

all_tables = [f for f in os.listdir(md_folder) if f.endswith('.md')]
pmid_to_tables = {}
for table in all_tables:
    pmid = table.split('_')[0]
    if pmid not in pmid_to_tables:
        pmid_to_tables[pmid] = []
    pmid_to_tables[pmid].append(table)
pmid_to_tables = dict(sorted(pmid_to_tables.items()))

# look at old attempts to come up with new name
version = next_available_version(dest_folder, namespace, '.md')

batch = []

if namespace.endswith('-tuned'):
    model_name = 'ft:gpt-4o-mini-2024-07-18:personal:oneshot:9sZYBFgF' # gpt-4o
else:
    model_name = 'gpt-4o'
for pmid, tables in pmid_to_tables.items():
    table_contents = []
    for filename in tables:
        with open(f'{md_folder}/{filename}', 'r', encoding='utf-8') as f:
            table_contents.append(f.read())
    # to retain pmid: 
    # custom_id.split('_', 2)[2]
    req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt_collections.table_oneshot_v1, table_contents, # table_varinvar_A_v1_2, table_contents, 
                                  model_name=model_name)
    batch.append(req)

write_to_jsonl(batch, f'batches/enzy/{namespace}_{version}.jsonl')
