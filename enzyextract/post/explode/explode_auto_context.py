import re
import pandas as pd
import yaml

from enzyextract.backform.backform_utils import fix_the_yaml, get_the_yamls
from enzyextract.utils import prompt_collections
from enzyextract.utils.construct_batch import get_batch_output, pmid_from_usual_cid, to_openai_batch_request
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls


def pmids_needing_exploding(matched_df: pd.DataFrame):
    # get rows where any of these are null: enzyme, substrate, organism
    
    rows = matched_df[matched_df[['enzyme', 'substrate', 'organism']].isnull().any(axis=1)]
    
    # get pmids, as str
    return rows['pmid'].unique().astype(str)

def fix_yaml_for_explode(yaml_block):
    fixed_yaml = ""
    mode = None
    for line in yaml_block.split("\n"):
        if line.startswith("data:"):
            mode = "data"
        elif line.startswith("context:"):
            mode = "context"
        if mode == "context" or line.startswith("data:") or line.startswith("    - descriptor:"):
            fixed_yaml += line + "\n" # only keep specific lines
    return fixed_yaml

def create_explode_batch(pmids: list[str], namespace: str, version: str, batch_yaml_path: str, prompt: str=prompt_collections.explode_1v0, model_name='gpt-4o'):
    # pmids = pmids_needing_exploding(matched_df)
    
    count = 0
    batch = []
    for custom_id, content, finish_reason in get_batch_output(batch_yaml_path):
        pmid = pmid_from_usual_cid(custom_id)
        
        content = re.sub('\bextras: ?\n', 'data:\n', content) # blunder
        if finish_reason == 'length':
            print("Too long:", pmid)
            continue
        
        if pmids is not None and pmid not in pmids:
            continue
        
        count += 1
                
        gpt_input = fix_the_yaml(content, fix_yaml_for_explode)
        docs = [gpt_input]

        req = to_openai_batch_request(f'{namespace}_{version}_{pmid}', prompt, docs, 
                                    model_name=model_name)
        batch.append(req)
    
    # submit batch
    return batch
    
        # for _, yaml in fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid)):

def parse_explode_message(ai_msg: str, debugpmid=None) -> tuple[pd.DataFrame, bool]:
    """Return tuple of (df, is_valid)"""
    yamls = get_the_yamls(ai_msg)
    if len(yamls) == 0:
        return None, False
    if len(yamls) > 1:
        print("Multiple YAMLs found in", debugpmid)
    # read, then turn into df
    obj = yaml.safe_load(yamls[0])
    df = pd.DataFrame(obj['data'])
    
    # take these columns (and create if they don't exist)
    # enzyme, organism, substrate, coenzymes, descriptor
    valid = True
    cols = ['enzyme', 'organism', 'substrate', 'cofactors', 'descriptor']
    for col in cols:
        if col not in df:
            df[col] = None
            valid = False
    df = df[cols]
    
    return df, valid
    
    
def infuse_explode_results(any_df: pd.DataFrame, explode_df: pd.DataFrame) -> pd.DataFrame:
    # any_df and explode_df both have enzyme, substrate, and organism columns
    # merge on basis of any_df's descriptor, using explode_df to fill in any blanks in any_df
    
    # merge on basis of descriptor
    
    targets = ['enzyme', 'organism', 'substrate', 'cofactors']
    # indiscriminately add _explode to explode_df
    explode_df.columns = [f'{col}_explode' if col in targets else col for col in explode_df.columns]
    
    explode_df = explode_df.drop_duplicates(subset=['descriptor', 'pmid']) # uh oh
    result = pd.merge(any_df, explode_df, how='left', on=['descriptor', 'pmid']) # , suffixes=('', '_explode'))
    
    assert len(result) == len(any_df), f"Length mismatch: {len(result)} vs {len(any_df)}"
    if 'cofactors' not in result:
        result['cofactors'] = None # result['cofactors_explode']
    #     result.drop(columns=['cofactors_explode'], inplace=True)
    # else:
        # targets.append('cofactors')
    # fill in blanks
    for col in targets:
        result[col] = result[col].fillna(result[f'{col}_explode'])
    
    # drop the _explode columns
    for col in targets:
        result.drop(columns=[f'{col}_explode'], inplace=True)
    return result
    
    