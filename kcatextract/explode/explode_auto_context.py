import pandas as pd

from kcatextract.utils.construct_batch import get_batch_output, pmid_from_usual_cid
from kcatextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls


def pmids_needing_exploding(matched_df: pd.DataFrame):
    # get rows where any of these are null: enzyme, substrate, organism
    
    rows = matched_df[matched_df[['enzyme', 'substrate', 'organism']].isnull().any(axis=1)]
    
    # get pmids
    return rows['pmid'].unique()

def submit_explode_request(matched_df: pd.DataFrame, batch_output_path: str):
    pmids = pmids_needing_exploding(matched_df)
    
    for custom_id, content, finish_reason in get_batch_output(batch_output_path):
        pmid = pmid_from_usual_cid(custom_id)
        
        content = content.replace('\nextras:\n', '\ndata:\n') # blunder
        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        totals += 1
        
        # for _, yaml in fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid)):
            
    
    
    