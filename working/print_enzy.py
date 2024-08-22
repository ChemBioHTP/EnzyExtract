import json
import os

from kcatextract.utils.construct_batch import get_batch_output, locate_correct_batch



if __name__ == "__main__":
    root = "completions/enzy"
    namespace = "brenwi-giveboth-tuneboth" # "rekcat-giveboth-4o" # "brenda-rekcat-md-v1-2"
    # version = "1"
    to_file = f"completions/enzy_tuned/{namespace}"
    at, version = locate_correct_batch(root, namespace) # , '1')
    print("Found version", version)
    # table_varinvar_A_v1_1
    result = """\
## Description:

prompt: table_oneshot_v1
papers: tableless
"""
    for custom_id, content, finish_reason in get_batch_output(f'{root}/{at}'):
        
        pmid = custom_id.split('_', 2)[2]
        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        result += f"## PMID: {pmid}\n\n"
        result += content + "\n\n"
    with open(f'{to_file}_{version}.md', 'w', encoding='utf-8') as f:
        f.write(result)
    
    
    
    