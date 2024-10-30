import pandas as pd

from kcatextract.fetch_sequences.get_closest_substrate import script_to_ask_gpt
from kcatextract.utils.construct_batch import to_openai_batch_request, write_to_jsonl
from kcatextract.utils.openai_management import process_env, submit_batch_file

def script0():
    
    dest_folder = 'batches/gptclose'
    namespace = "brenda-rekcat-tuneboth-2"
    version = 2
    checkpoint_df = pd.read_csv(f"C:/conjunct/enzy_runner/data/_post_sequencing/sequenced_explode-for-{namespace}_{version}.tsv",
    sep='\t')
    checkpoint_df = checkpoint_df.astype({'pmid': str})
    brenda_substrate_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")

    batch = script_to_ask_gpt(checkpoint_df, brenda_substrate_df, namespace=f"gptclose-for-{namespace}_{version}")
    
    will_write_to = f'{dest_folder}/gptclose-for-{namespace}_{version}.jsonl'
    write_to_jsonl(batch, will_write_to)
    
    process_env('.env')
    submit_batch_file(will_write_to, pending_file='batches/pending.jsonl')

if __name__ == '__main__':
    script0()
    