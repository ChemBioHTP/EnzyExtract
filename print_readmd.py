import json
import os

import pandas as pd

from kcatextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df


def convert_md_to_csv(md_filepath, write_dest):
    with open(md_filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    valids = []
    _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=None))
    for pmid, yaml in _generator: # 
        # assert _ == None
        
        df, context = yaml_to_df(yaml, auto_context=True, debugpmid=pmid) # pmid, silence debug
        df['pmid'] = pmid
        if df.empty:
            continue
        valids.append(df)
        # valid_pmids.add(str(pmid)) # needs to be a set
    
    valid_df = pd.concat(valids)
    valid_df.to_csv(write_dest, index=False)

if __name__ == '__main__':
    convert_md_to_csv('prints/apogee_runeem_20241025.md', 'data/humaneval/apogee_runeem_20241025.csv')