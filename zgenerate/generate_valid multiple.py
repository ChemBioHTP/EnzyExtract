
import json
import os
import pandas as pd
from enzyextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
from enzyextract.utils.yaml_process import extract_yaml_code_blocks, fix_multiple_yamls, yaml_to_df, equivalent_from_json_schema
from enzyextract.hungarian.csv_fix import clean_columns_for_valid


def generate_valid_parquet(namespace, # tuned # tableless-oneshot # brenda-rekcat-md-v1-2
        filenames, # list of filenames
        *,
        compl_folder = 'completions/enzy', # C:/conjunct/table_eval/completions/enzy
        silence = True,
        use_yaml=True,
        use_cached=True
):
    """
    Warning: if a blacklist/whitelist is provided, the cached matched csv will only contain those which pass.
    """
    

    validated_csv = f'data/valid/_valid_{namespace}.parquet'
    
    valids = [] # list of dataframes
    valid_pmids = set()
    total_ingested = 0
    

    # if use_cached and validated_csv and os.path.exists(validated_csv):
    #     print("Already exists. Using cached valid_csv.")
    #     return pd.read_csv(validated_csv), {}
    for filename in filenames:
        for custom_id, content, finish_reason in get_batch_output(f'{compl_folder}/{filename}'):
            pmid = str(pmid_from_usual_cid(custom_id))
            
            content = content.replace('\nextras:\n', '\ndata:\n') # blunder
            if finish_reason == 'length':
                print("Too long:", pmid)
                continue
            total_ingested += 1
            
            if use_yaml:
                _generator = fix_multiple_yamls(yaml_blocks=extract_yaml_code_blocks(content, current_pmid=pmid))
            else:
                # assume json content
                _generator = [(0, equivalent_from_json_schema(content))]
            for _, yaml in _generator: # 
                
                df, context = yaml_to_df(yaml, auto_context=True, debugpmid=None if silence else pmid) # pmid, silence debug
                df['pmid'] = pmid
                if df.empty:
                    continue
                valids.append(df)
                valid_pmids.add(str(pmid)) # needs to be a set

    
    valid_df = clean_columns_for_valid(pd.concat(valids)) # bad units for km and kcat are rejected here
    valid_df = valid_df.astype({'pmid': 'str'})
    
    # if validated_csv and not os.path.exists(validated_csv):
    print("Writing to", validated_csv)

    if validated_csv: #  and not os.path.exists(validated_csv):
        print("Writing to", validated_csv)
        if validated_csv.endswith('.parquet'):
            import polars as pl
            pl.from_pandas(valid_df).write_parquet(validated_csv)
        else:
            valid_df.to_csv(validated_csv, index=False)
    
    return valid_df

if __name__ == "__main__":
    blacklist = whitelist = None

    # namespace = 'apogee-rebuilt'
    # namespace = 'apatch-topoff-open'
    # namespace = 'bucket-rebuilt'
    namespace = 'apatch-rebuilt'

    structured = namespace.endswith('-str') or namespace.endswith('-struct')
    version = None
    # compl_folder = 'completions/enzy/apogee'
    # compl_folder = 'completions/enzy/bucket'
    compl_folder = 'completions/enzy/apatch'

    from enzyextract.utils.openai_management import merge_all_chunked_completions
    merge_all_chunked_completions(compl_folder, compl_folder)


    filenames = [f for f in os.listdir(compl_folder) if f.endswith('.jsonl') and not f.endswith('0.jsonl') and not f.endswith('.429.jsonl')]
    print("Collected", len(filenames), "filenames")
    df = generate_valid_parquet(namespace=namespace, filenames=filenames, 
                compl_folder=compl_folder, 
                silence=False,
                use_yaml=not structured)
