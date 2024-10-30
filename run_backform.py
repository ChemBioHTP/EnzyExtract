
import json
import pandas as pd

from kcatextract.backform.backform_utils import openai_batch_to_finetune, openai_crafted_batch_to_finetune, train_test_split, save_partitions
from kcatextract.backform.quality_assure import quality_assure_finetune, quality_assure_for_enzyme_matching, quality_assure_for_explode
from kcatextract.backform.get_perfects import get_perfects_only
from kcatextract.utils import prompt_collections
from kcatextract.utils.construct_batch import pmid_from_usual_cid
from kcatextract.utils.md_management import read_md_by_pmid

# exclude: 15248782



def script0():
    blacklist = set(['15248782'])

    # now open the original input batch and original output batch
    INPUT_BATCH = 'batches/explode/explode-for-brenda-rekcat-tuneboth-2_2.jsonl'
    OUTPUT_BATCH = 'completions/explode/explode-for-brenda-rekcat-tuneboth-2_2.jsonl'
    
    DEST_FOLDER = 'backform/finetunes'
    NAMESPACE = 'explode-for-brenda-rekcat-tuneboth-2_2'

    with open(INPUT_BATCH, 'r') as f:
        input_batch = [json.loads(line) for line in f]
    with open(OUTPUT_BATCH, 'r') as f:
        output_batch = [json.loads(line) for line in f]

    # now, for each pmid in perfect_pmids, find the corresponding input and output
    pmid_to_input = {}
    pmid_to_output = {}
    for item in input_batch:
        pmid = pmid_from_usual_cid(item['custom_id'])
        pmid_to_input[pmid] = item

    for item in output_batch:
        pmid = pmid_from_usual_cid(item['custom_id'])
        if item['response']['body']['choices'][0]['finish_reason'] == 'length':
            print(f"Too long: {pmid}")
            continue
        pmid_to_output[pmid] = item

    # now, for each pmid, 
    target_pmids = pmid_to_input.keys() & pmid_to_output.keys() - blacklist
    result = []
    for pmid in target_pmids:
        input_item = pmid_to_input[pmid]
        output_item = pmid_to_output[pmid]
        req = openai_batch_to_finetune(input_item, output_item, system_prompt=prompt_collections.explode_1v3)
        result.append((pmid, req))
    
    print("We started with", len(target_pmids)) # 766 is still sufficient
    # quality assure
    good = []
    for i, (pmid, req) in enumerate(result):
        problems = quality_assure_for_explode(req)
        if problems:
            print(f"In pmid {pmid}:", ', '.join(problems))
        else:
            good.append((pmid, req))

    print("After that, we have", len(good)) # 766 is still sufficient
    
    # pure_requests = [req for pmid, req in good]
    train, val, test = train_test_split(good, train_ratio=0.5, val_ratio=0.2, seed=42)
    save_partitions(train, val, test, DEST_FOLDER, NAMESPACE)
    # write pmids separately
    with open(f"{DEST_FOLDER}/pmids-{NAMESPACE}.train.txt", 'w') as f:
        for pmid, req in train:
            f.write(str(pmid) + '\n')
    with open(f"{DEST_FOLDER}/pmids-{NAMESPACE}.val.txt", 'w') as f:
        for pmid, req in val:
            f.write(str(pmid) + '\n')
    with open(f"{DEST_FOLDER}/pmids-{NAMESPACE}.test.txt", 'w') as f:
        for pmid, req in test:
            f.write(str(pmid) + '\n')

if __name__ == "__main__":
    script0()