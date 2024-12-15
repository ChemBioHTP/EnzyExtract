import json

from enzyextract.backform.backform_utils import openai_crafted_batch_to_finetune
from enzyextract.utils.construct_batch import pmid_from_usual_cid
from enzyextract.utils.md_management import read_md_by_pmid


def script2():
    # exfiltrate input batch and the documents
    md_path = 'completions/enzy_tuned/giveboth-cofactor-4o_2 train v3.md'
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    pmids = []
    for pmid, content in read_md_by_pmid(content):
        pmids.append(pmid)


    attention_needed = ['11382747', '11741974', '11948179', '11880657', '11820932', '11369858', '11468288', '11209758']

    input_batch = 'batches/enzy/rekcat-giveboth-4o_2.jsonl'
    output_reqs_good = []
    output_reqs_bad = []
    with open(input_batch, 'r') as f:
        for line in f:
            obj = json.loads(line)
            pmid = str(pmid_from_usual_cid(obj['custom_id'])) # blunder
            if pmid in pmids:
                if pmid in attention_needed:
                    output_reqs_bad.append(line)
                else:
                    output_reqs_good.append(line)
    
    # save the output_reqs
    with open('batches/enzy/backform/t4neboth.good.jsonl', 'w') as f:
        for line in output_reqs_good:
            f.write(line)
    
    with open('batches/enzy/backform/t4neboth.bad.jsonl', 'w') as f:
        for line in output_reqs_bad:
            f.write(line)

    

if __name__ == "__main__":
    script2()