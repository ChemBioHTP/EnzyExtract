import glob
import os
import pandas as pd
import pymupdf

from enzyextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
import enzyextract.utils.prompt_collections as prompt_collections
from enzyextract.utils.working import pmid_to_tables_from
    
# setup
md_folder = 'C:/conjunct/tmp/brenda_rekcat_tables/md_v3'
dest_folder = 'batches/enzy'
# namespace = 'brenda-rekcat-md-v1-2'
# namespace = 'tablemd-oneshot-tuned'
namespace = 'tablemd-suite-train'
prompt = prompt_collections.table_oneshot_v1_1
use_pdfs = True
pdf_folder = 'C:/conjunct/tmp/brenda_rekcat_pdfs'


if '_' in namespace:
    raise ValueError("Namespace cannot contain underscores")

pmid_to_tables = pmid_to_tables_from(md_folder)

# _whitelist_df = pd.read_csv('C:/conjunct/vandy/yang/corpora/eval/rekcat/rekcat_checkpoint_5 - rekcat_ground_64.csv')
# whitelist = set([str(int(x)) for x in _whitelist_df['pmid']])
# pmid_to_tables = {k: v for k, v in pmid_to_tables.items() if k in whitelist}

# look at old attempts to come up with new name
version = next_available_version(dest_folder, namespace, '.md')

batch = []

if namespace.endswith('-tuned'):
    prompt = prompt_collections.table_oneshot_v1
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
    if use_pdfs:
        doc = pymupdf.open(f"{pdf_folder}/{pmid}.pdf")
        for page in doc:
            table_contents.append(page.get_text("text"))
        
    
    req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, table_contents, # table_varinvar_A_v1_2, table_contents, 
                                  model_name=model_name)
    batch.append(req)

write_to_jsonl(batch, f'batches/enzy/{namespace}_{version}.jsonl')
