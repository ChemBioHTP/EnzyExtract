
import json
import os
import re

import pandas as pd
from enzyextract.fetch_sequences.confirm_enzyme_sequences import construct_window, str_to_splitable
from enzyextract.utils import prompt_collections
from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid, to_openai_batch_request, write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls


compl_folder = 'completions/enzy'
src_namespace = 'tableless-oneshot'
src_version = None

dest_namespace = 'tableless-enzymes'
dest_folder = 'batches/enzymes'
dest_version = None

filename, src_version = locate_correct_batch(compl_folder, src_namespace, version=src_version) # , version=1)
dest_version = next_available_version(dest_folder, dest_namespace, '.jsonl')

pmid2yaml_json = f'_debug/_cache_json/_{src_namespace}_{src_version}.json'

if pmid2yaml_json and os.path.exists(pmid2yaml_json):
    print("Reading from", pmid2yaml_json)
    with open(pmid2yaml_json) as f:
        pmid2yaml = json.load(f)
else:
    pmid2yaml = {}
    for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
        pmid = str(pmid_from_usual_cid(custom_id))
        
        content = content.replace('\nextras:\n', '\ndata:\n') # blunder

        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        
        enzymes_re = re.compile(r"^    enzymes: ?$")
        finish_re = re.compile(r"^    [^ ]")

        for _, myyaml in fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid)):
            # just obtain the "enzymes" key
            # obj = yaml.safe_load(myyaml)
            builder = ""
            within = False
            for line in myyaml.split("\n"):
                if enzymes_re.match(line):
                    within = True
                elif within and finish_re.match(line):
                    within = False
                if within:
                    assert line.startswith("    ")
                    builder += line[4:] + "\n" #de-indenet
            pmid2yaml[pmid] = builder
    print("Writing to", pmid2yaml_json)
    with open(pmid2yaml_json, 'w') as f:
        json.dump(pmid2yaml, f)



pmid2seq = pd.read_csv("fetch_sequences/results/rekcat_enzymes.tsv", sep="\t")

pmid2seq['pmid'] = pmid2seq['filename'].apply(lambda x: x.rsplit(".pdf", 1)[0])

# for col in ['pdb', 'uniprot', 'refseq', 'genbank']:
#     pmid2seq[col] = pmid2seq[col].apply(str_to_splitable)

uniprot_df = pd.read_csv("fetch_sequences/results/rekcat_uniprots.tsv", sep="\t")
pdb_df = pd.read_csv("fetch_sequences/results/rekcat_pdbs.tsv", sep="\t")

# print first 5 pmids   
pmid2seq.replace('', pd.NA, inplace=True)
pmid2seq.dropna(subset=['pdb', 'uniprot', 'refseq', 'genbank'], how='all', inplace=True)
pmid2seq.replace(pd.NA, '', inplace=True)
# not_all_null = pmid2seq.filter(['pdb', 'uniprot', 'refseq', 'genbank']).applymap(lambda x: len(x) > 0).any(axis=1)


candidates = pmid2seq['pmid'].unique().tolist()

print("These many candidates: ", len(candidates))

# ct = 10
batch = []
for pmid in candidates:
    if pmid not in pmid2yaml:
        continue
    result = construct_window(pmid, pmid2yaml[pmid], pmid2seq, uniprot_df, pdb_df)
    if result:
        # print(result)
        req = to_openai_batch_request(f'{dest_namespace}_{dest_version}_{pmid}', prompt_collections.confirm_enzymes_v1, [result], 
                                model_name='gpt-4o')
        batch.append(req)

        # ct -= 1
    # if ct <= 0:
        # break
write_to_jsonl(batch, f'{dest_folder}/{dest_namespace}_{dest_version}.jsonl')
