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


process_env('.env')

def script0():
    # submit fragmentary
    namespace = 'scratch-open-t2neboth'

    version = 2

    for filepath in glob.glob(f"batches/enzy/{namespace}_{version}.jsonl"):
        

        batchname = submit_batch_file(filepath, pending_file='batches/pending.jsonl') # will ask for confirmation

    for filepath in glob.glob(f"batches/enzy/{namespace}_{version}.*.jsonl"):
        
        # replace \\ with /
        filepath = filepath.replace('\\', '/')
        batchname = submit_batch_file(filepath, pending_file='batches/pending.jsonl') # will ask for confirmation
    
def script1():
    # merge fragments
    from kcatextract.utils.openai_management import merge_chunked_completions
    folder = 'completions/enzy/apogee'
    
    merge_chunked_completions('brenda-open-apogee-t2neboth', 1, compl_folder=folder, dest_folder=folder)

def script2():
    from kcatextract.utils.openai_management import merge_all_chunked_completions
    folder = 'completions/enzy'
    merge_all_chunked_completions(folder, folder)

if __name__ == '__main__':
    script2()
    