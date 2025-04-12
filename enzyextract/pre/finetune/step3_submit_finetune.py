"""
Original location: working/working_backform.py
"""

# back construct: given a corrected csv, construct the ideal yaml
# use this to fine tune gpt-4o-mini

import json
import re
import pandas as pd

from enzyextract.backform.backform_utils import openai_batch_to_finetune, openai_crafted_batch_to_finetune
from enzyextract.backform.quality_assure import quality_assure_finetune, quality_assure_for_enzyme_matching
from enzyextract.metrics.get_perfects import get_perfects_only
from enzyextract.utils import prompt_collections
from enzyextract.submit.batch_utils import pmid_from_usual_cid
from enzyextract.utils.md_management import read_md_by_pmid


def train_test_split(result, train_ratio=0.7, val_ratio=0.2, seed=42):
    
    # shuffle
    import random
    random.seed(seed)
    random.shuffle(result)
    
    train = result[:int(len(result) * train_ratio)]
    val = result[int(len(result) * train_ratio):int(len(result) * (train_ratio + val_ratio))]
    test = result[int(len(result) * (train_ratio + val_ratio)):]
    return train, val, test

def save_partitions(train, val, test, dest_folder, namespace, pmids_dest=None):
    for part, name in [(train, 'train'), (val, 'val'), (test, 'test')]:
        if not part:
            continue
        with open(f"{dest_folder}/{namespace}.{name}.jsonl", 'w') as f:
            for pmid, item in part:
                f.write(json.dumps(item) + '\n')
        if pmids_dest:
            with open(f"{pmids_dest}/pmids-{namespace}.{name}.txt", 'w') as f:
                for pmid, item in part:
                    f.write(str(pmid) + '\n')
    


def script_finetune_from_md(
    namespace,
    input_batch,
    input_md_path,
):
    # backform from a md file
    

    input_reqs = {}
    with open(input_batch, 'r') as f:
        for line in f:
            obj = json.loads(line)
            pmid = str(pmid_from_usual_cid(obj['custom_id'])) # blunder
            input_reqs[pmid] = obj
    
    with open(input_md_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    
    train = []
    val = []

    # select those with little educational value but good verificational value
    val_set = [10206992, 10347221, 10373434, 10947957, 10960485, 11016923, 11468288, 11675384, 12054464, 12604203]


    has_table_re = re.compile(r'\|:?-*:?(\|:?-*:?)+\|')
    for pmid, content in read_md_by_pmid(content):
        assert pmid in input_reqs
        input_req = input_reqs[pmid]
        req = openai_crafted_batch_to_finetune(input_req, content, system_prompt=prompt_collections.for_manifold)
        
        # quality assurance
        
        if 'μM' in content or 'µM' in content:
            # then the input req better have it too!
            tabled = ""
            untabled = ""
            # look at input req
            for msg in input_req['body']['messages'][1:]:
                content = msg['content']
                if has_table_re.search(content):
                    tabled += content
                else:
                    untabled += content
            if tabled and 'μ' not in tabled and 'µ' not in tabled:
                print(pmid, "does not have µM in table")
            if untabled and 'μ' not in untabled and 'µ' not in untabled:
                print(pmid, "does not have µM in untabled")
            if not tabled:
                print(pmid, "does not have table")
            
            
        if pmid in val_set:
            val.append((pmid, req))
        else:
            train.append((pmid, req))
    # assert len(val) == len(val_set)
    
    # quality assurance. 

    dest_folder = 'zfinetune/jobs'
    save_partitions(train, val, [], dest_folder, namespace)
    
    
if __name__ == "__main__":
    namespace = 't2neboth'
    input_batch = 'zfinetune/inputs/t2neboth_tune.jsonl'
    input_md_path = 'zfinetune/mds/train t2neboth.md'
    script_finetune_from_md(
        namespace,
        input_batch,
        input_md_path,
    )