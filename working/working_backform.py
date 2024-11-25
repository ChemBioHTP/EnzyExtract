# back construct: given a corrected csv, construct the ideal yaml
# use this to fine tune gpt-4o-mini

import json
import pandas as pd

from kcatextract.backform.backform_utils import openai_batch_to_finetune, openai_crafted_batch_to_finetune
from kcatextract.backform.quality_assure import quality_assure_finetune, quality_assure_for_enzyme_matching
from kcatextract.metrics.get_perfects import get_perfects_only
from kcatextract.utils import prompt_collections
from kcatextract.utils.construct_batch import pmid_from_usual_cid
from kcatextract.utils.md_management import read_md_by_pmid

# from backform.get_perfects import get_perfects_only
# current_best = pd.read_csv('completions/enzy/rekcat-vs-brenda_5.csv')
# perfect_df = get_perfects_only(current_best)
# print(perfect_df['kcat'].count())

# print(len(set(perfect_df['pmid'])))
# print(perfect_df['pmid'].unique())

# perfect_df.to_csv('backform/rekcat-vs-brenda_5_perfect.csv', index=False)

# get # of kcat


# includes: 9733678, 9628739, 9521731, 9305868, 9235932


# for these pmids, prefer the manually corrected csv: (RHS, corrected brenda)
# 9696781, 9733738, 9790663, 9857017, 9933602

# 9636048, 9576908, 9556600, 9495750, 9398292, 9359420, 9202000, 8948426, 8910590, 8670160, 8626758

# for these pmids, prefer the manually corrected csv: (RHS, corrected brenda) BUT only take non-null kcat:

# 8780523

# for these pmids, prefer the direct gpt-4o output:
# 9973343, 9092497, 8939970, 8645224

# for these pmids, rearrange in private:
# 8688421

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
    

# ok, let's use the perfect pmids identified from part B
def script0():
    # backform, for oneshot
    oneshot_df = pd.read_csv('completions/enzy/tableless-oneshot_1.csv') # this thing has the stri
    
    # aim for perfection
    print("We started with", len(oneshot_df['pmid'].unique()), "pmids.")
    perfect_df = get_perfects_only(oneshot_df)
    
    perfect_pmids = perfect_df['pmid'].unique()
    print("We have", len(perfect_pmids), "perfect pmids to work with.")
    print("Which is", len(perfect_df), "Km.")

    # now open the original input batch and original output batch
    INPUT_BATCH = 'batches/enzy/tableless-oneshot_1.jsonl'
    OUTPUT_BATCH = 'completions/enzy/batch_klff5wdwPFcQ2Mf3j2z9vr8B_output.jsonl'
    
    DEST_FOLDER = 'backform/finetunes'
    NAMESPACE = 'tableless-oneshot_2'

    with open(INPUT_BATCH, 'r') as f:
        input_batch = [json.loads(line) for line in f]
    with open(OUTPUT_BATCH, 'r') as f:
        output_batch = [json.loads(line) for line in f]

    # now, for each pmid in perfect_pmids, find the corresponding input and output
    pmid_to_input = {}
    pmid_to_output = {}
    for item in input_batch:
        pmid = int(pmid_from_usual_cid(item['custom_id']))
        if pmid in perfect_pmids:
            pmid_to_input[pmid] = item

    for item in output_batch:
        pmid = int(pmid_from_usual_cid(item['custom_id']))
        if pmid in perfect_pmids:
            pmid_to_output[pmid] = item

    # now, for each pmid, 
    result = []
    for pmid in perfect_pmids:
        input_item = pmid_to_input[pmid]
        output_item = pmid_to_output[pmid]
        req = openai_batch_to_finetune(input_item, output_item, system_prompt=prompt_collections.table_oneshot_v1)
        result.append((pmid, req))

    # quality assure
    good = []
    for i, (pmid, req) in enumerate(result):
        problems = quality_assure_finetune(req)
        if problems:
            print(f"In pmid {pmid}:", ', '.join(problems))
        else:
            good.append((pmid, req))

    print("After that, we have", len(good)) # 766 is still sufficient
    
    # pure_requests = [req for pmid, req in good]
    train, val, test = train_test_split(good, train_ratio=0.4, val_ratio=0.1, seed=42)
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


def script1():
    """monitor the number and ratio of perfects"""
    disallowed_pmids = set()
    with open("backform/finetunes/tableless-oneshot-pmids.train.txt") as f:
        disallowed_pmids = set(f.read().splitlines())
    disallowed_pmids = set([int(pmid) for pmid in disallowed_pmids])
    
    # CSV = 'completions/enzy/tableless-oneshot_1.csv'
    # CSV = 'completions/enzy_tuned/tableless-oneshot-tuned_1.csv'
    CSV = 'completions/enzy_tuned/tableless-oneshot-mini_1.csv'
    
    oneshot_df = pd.read_csv(CSV) # this thing has the stri
    
    # we must exclude training pmids from oneshot_df
    oneshot_df = oneshot_df[~oneshot_df['pmid'].isin(disallowed_pmids)]
    
    # aim for perfection
    print("We started with", len(oneshot_df['pmid'].unique()), "pmids.")
    perfect_df = get_perfects_only(oneshot_df)
    
    perfect_pmids = perfect_df['pmid'].unique()
    print("We have", len(perfect_pmids), "pmids to work with.")
    print("Which is", len(perfect_df), "Km.")

    # now open the original input batch and original output batch
    # INPUT_BATCH = 'batches/enzy/tableless-oneshot_1.jsonl'
    # OUTPUT_BATCH = 'completions/enzy/batch_klff5wdwPFcQ2Mf3j2z9vr8B_output.jsonl'
    
    # INPUT_BATCH = 'batches/enzy/tableless-oneshot-tuned_1.jsonl'
    # OUTPUT_BATCH = 'completions/enzy_tuned/batch_8gS8mFOQFulVV42XrdCco1Sq_output.jsonl'
    
    INPUT_BATCH = 'batches/enzy/tableless-oneshot-mini_1.jsonl'
    OUTPUT_BATCH = 'completions/enzy_tuned/batch_nGafHlk7f4pMqlPNT3T09YIs_output.jsonl'
    

    with open(INPUT_BATCH, 'r') as f:
        input_batch = [json.loads(line) for line in f]
    with open(OUTPUT_BATCH, 'r') as f:
        output_batch = [json.loads(line) for line in f]

    # now, for each pmid in perfect_pmids, find the corresponding input and output
    pmid_to_input = {}
    pmid_to_output = {}
    for item in input_batch:
        pmid = int(pmid_from_usual_cid(item['custom_id']))
        if pmid in perfect_pmids:
            pmid_to_input[pmid] = item

    for item in output_batch:
        pmid = int(pmid_from_usual_cid(item['custom_id']))
        if pmid in perfect_pmids:
            pmid_to_output[pmid] = item

    # now, for each pmid, 
    result = []
    for pmid in perfect_pmids:
        input_item = pmid_to_input[pmid]
        output_item = pmid_to_output[pmid]
        if output_item['response']['body']['choices'][0]['finish_reason'] == 'length':
            print("Too long:", pmid)
            continue
        req = openai_batch_to_finetune(input_item, output_item, system_prompt=prompt_collections.table_oneshot_v1)
        result.append((pmid, req))

    # quality assure
    good = []
    for i, (pmid, req) in enumerate(result):
        problems = quality_assure_finetune(req)
        if problems:
            print(f"In pmid {pmid}:", ', '.join(problems))
        else:
            good.append((pmid, req))

    print("After that, we have", len(good)) # 766 is still sufficient
    # print first 5
    for pmid, req in good[:5]:
        print(pmid)
    

# create backform for enzyme sequence matching
def script2():
    # now open the original input batch and original output batch
    input_batch = 'batches/enzymes/tableless-enzymes_1.jsonl'
    output_batch = 'completions/enzymes/batch_FULu6DO6WAaqQQFN70v8iQ0z_output.jsonl'
    
    dest_folder = 'backform/finetunes'
    pmids_dest = 'C:/conjunct/vandy/yang/corpora/manifest/auto/finetunes'
    namespace = 'confirm-enzymes'
    
    input_reqs = {}
    with open(input_batch, 'r') as f:
        # input_batch = [json.loads(line) for line in f]
        for line in f:
            obj = json.loads(line)
            pmid = int(pmid_from_usual_cid(obj['custom_id']))
            input_reqs[pmid] = obj
    
    output_reqs = {}
    with open(output_batch, 'r') as f:
        # output_batch = [json.loads(line) for line in f]
        for line in f:
            obj = json.loads(line)
            pmid = int(pmid_from_usual_cid(obj['custom_id']))
            output_reqs[pmid] = obj
        
    result = []
    for pmid in input_reqs:
    # for input_item, output_item in zip(input_batch, output_batch):
        input_item = input_reqs[pmid]
        output_item = output_reqs[pmid]
        req = openai_batch_to_finetune(input_item, output_item, system_prompt=prompt_collections.confirm_enzymes_v1_1)
        result.append((pmid, req))
    
    # quality assure
    # perfect_idents = ["1CLI", "1TSN", "1FGS", "7CEL", "1DIK", "1GW6", "1JVG", "1A4I", "1M1B", "1RSN", "1LWD", "1PTU", "AF505789", "1J58", "1JTK", "1F3T", "1C7O", "1LWD", "1X74", "1WY5", "1LWD", "2AL1", "2HA0", "2IO7", "2GZ6", "1J58", "1FK8", "Q15126", "2VJL", "2HU8", "3C4Z", "3BUR", "AY533175", "3CSJ", "1SYL", "3DJD", "1OE1", "2J0F", "FJ439676", "XP_002312315", "1FK8", "1JU3", "1YPV", "1OH9", "2BX4", "1XNY", "2XFN", "XP_001836356", "O46411", "1C9U", "3B5Z", "2XVE", "4A3Y", "1D8D_2", "4GVQ", "4LQL", "3FSN", "1JQI", "4PFK", "3ZPH", "ABQ81648.1", "3RFT", "1DKL", "2XLL", "6V0A", "3IHG", "1JTK", "1NJE", "1ECG", "1TRK", "1PSO", "1BE0"]
    # perfect_idents += ["1STD", "1CLI", "1DIK", "1GW6", "1JV1", "1A4I", "1M1B", "2HNP", "1LCI", "1UW8", "1JTK", "2AAI_2", "1LWD", "1WY5", "1LWD", "2AL1", "2HA2", "2IO9", "2GZ6", "1C3U", "1UW8", "Q15126", "1P5H", "3CSJ", "3DJE", "1OE1", "2J0F", "1FK8", "P15244", "1L7R", "3EJL", "1OHA", "2BX4", "3C3U", "2XFN", "O46411", "1C9U", "1NAL", "2XVF", "2CYD", "1D8D_2", "4GVQ", "4LQL", "2HA4", "3FSN", "P04424", "1EBH", "1ZK7", "3ZPH", "P04181", "3RFT", "P20933", "6MOR", "NP_415804.1", "ABN09948.3", "1JTK", "1NJE", "1ECF"]
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    # dists_df = dists_df[dists_df.apply(lambda x: x['desire'].str.replace(), axis=1)]
    perf_df = dists_df[(dists_df['desire'].str.replace('.', '', regex=False).str.len() >= 2) | (dists_df['distance'] == 0)]
    perfect_idents = set(perf_df['ident'].str.split('_', n=1).str[0])

    good = []
    for i, (pmid, req) in enumerate(result):
        problems = quality_assure_for_enzyme_matching(req, golden_idents=perfect_idents)
        if problems:
            print(f"In {pmid}:", ', '.join(problems))
        else:
            good.append((pmid, req))
    print(f"QA Results: {len(result)} --> {len(good)}")            
    
    train, val, test = train_test_split(good, train_ratio=0.25, val_ratio=0.25, seed=42)
    save_partitions(train, val, test, dest_folder, namespace, pmids_dest=pmids_dest)
    # write pmids separately

def script3():
    # backform from a md file
    
    # now open the original input batch and original output batch
    # input_batch = 'batches/enzy/rekcat-giveboth-4o_2.jsonl'
    input_batch = 'batches/enzy/backform/t4neboth.manual.jsonl'
    input_reqs = {}
    with open(input_batch, 'r') as f:
        for line in f:
            obj = json.loads(line)
            pmid = str(pmid_from_usual_cid(obj['custom_id'])) # blunder
            input_reqs[pmid] = obj
    
    # backform from a md file
    val_set = [10206992, 10347221, 10373434, 10947957, 10960485, 11016923, 11468288, 11675384, 12054464, 12604203]
    # md_path = 'completions/enzy_tuned/rekcat-giveboth-4o_2 train.md'
    # md_path = 'completions/enzy_tuned/giveboth-cofactor-4o_2 train v3.md'
    md_path = 'completions/enzy_tuned/train t4neboth.md'
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()
    train = []
    val = []
    for pmid, content in read_md_by_pmid(content):
        assert pmid in input_reqs
        input_req = input_reqs[pmid]
        req = openai_crafted_batch_to_finetune(input_req, content, system_prompt=prompt_collections.table_oneshot_v3)
        if int(pmid) in val_set:
            val.append((pmid, req))
        else:
            train.append((pmid, req))
    assert len(val) == len(val_set)
    
    
    dest_folder = 'backform/finetunes'
    pmids_dest = 'C:/conjunct/vandy/yang/corpora/manifest/auto/finetunes'
    namespace = 't4neboth'
    save_partitions(train, val, [], dest_folder, namespace, pmids_dest=pmids_dest)
    
    
if __name__ == "__main__":
    script3()