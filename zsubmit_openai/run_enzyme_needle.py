# process organisms and get canonical
from enzyextract.submit.batch_utils import to_openai_batch_request, write_to_jsonl
from enzyextract.utils import prompt_collections
from enzyextract.submit.openai_management import process_env, submit_batch_file

import pandas as pd

def script0():
    """
    Script: canonicalize organisms
    """
    df = pd.read_csv('data/_compiled/nonbrenda.tsv', sep='\t')

    # get organisms
    organisms = df.organism.unique()
    # for org in organisms:
    #     print(org)
    # print(organisms)



    def to_doc(batch: list):
        result = ""
        for org in batch:
            result += f'{org}\n'
        return result
        
            
    
    namespace = 'nonbrenda-organism_1'
    dest_folder = 'batches/enzymatch'
    # split into batches of 4
    pool = []
    for i in range(0, len(organisms), 4):
        batch = organisms[i:i+4]
        req = to_openai_batch_request(f'nonbrenda-organism_1', prompt_collections.prompt_organism_v1_1, 
                                      docs=[to_doc(batch)], model_name='gpt-4o')
        pool.append(req)

    # save to enzymatch
    write_to_jsonl(pool, f'{dest_folder}/{namespace}.jsonl')
    
    print(f'Written to {dest_folder}/{namespace}.jsonl')
    batchname = submit_batch_file(f'{dest_folder}/{namespace}.jsonl')

if __name__ == '__main__':
    process_env('.env')
    script0()