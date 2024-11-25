# back construct: given a corrected csv, construct the ideal yaml
# use this to fine tune gpt-4o-mini

import json
import pandas as pd

from backform.backform_utils import openai_batch_to_finetune, quality_assure_finetune
from utils import prompt_collections
from utils.construct_batch import pmid_from_usual_custom_id

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


# ok, let's use the perfect pmids identified from part B
perfect_df = pd.read_csv('completions/enzy_improve/rekcat-tableless-perfect.csv')
perfect_pmids = perfect_df['pmid'].unique()

# now open the original input batch and original output batch
INPUT_BATCH = 'batches/enzy/improve/brenda-rekcat-partB_1.jsonl'
OUTPUT_BATCH = 'completions/enzy_improve/batch_aMrNnoybl7FxNiYfUA1EKKsH_output.jsonl'

with open(INPUT_BATCH, 'r') as f:
    input_batch = [json.loads(line) for line in f]
with open(OUTPUT_BATCH, 'r') as f:
    output_batch = [json.loads(line) for line in f]

# now, for each pmid in perfect_pmids, find the corresponding input and output
pmid_to_input = {}
pmid_to_output = {}
for item in input_batch:
    pmid = pmid_from_usual_custom_id(item['custom_id'])
    if pmid in perfect_pmids:
        pmid_to_input[pmid] = item

for item in output_batch:
    pmid = pmid_from_usual_custom_id(item['custom_id'])
    if pmid in perfect_pmids:
        pmid_to_output[pmid] = item

# now, for each pmid, 
result = []
for pmid in perfect_pmids:
    input_item = pmid_to_input[pmid]
    output_item = pmid_to_output[pmid]
    req = openai_batch_to_finetune(input_item, output_item, system_prompt=prompt_collections.table_improve_v1_1)
    result.append((pmid, req))

# quality assure
good = []
for i, (pmid, req) in enumerate(result):
    flag = quality_assure_finetune(req)
    if flag:
        print(f"(problem with pmid {pmid})")
    else:
        good.append((pmid, req))

print(len(good)) # 72 is still sufficient