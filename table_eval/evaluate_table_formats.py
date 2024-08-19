import random
import string
import pandas as pd

from construct_batch import to_openai_batch_request
import prompt_collections



# for batch 3, the seed is 42
# for batch 4, the seed is 41
random.seed(41) 
def create_decks(num_decks, num_rows=1, num_cols=20):
    deck = list(string.ascii_lowercase)
    
    # shuffle
    dfs = []
    for _ in range(num_decks):
        builder = []
        for _ in range(num_rows + 1): # +1 for the header
            random.shuffle(deck)
            builder.append(deck[:num_cols].copy())
        df = pd.DataFrame(builder[1:], columns=builder[0])
        dfs.append(df)
    return dfs

def designate_target(df, i, target='TARGET'):
    df = df.copy()
    # set the i-th header to TARGET
    df.rename(columns={df.columns[i]: target}, inplace=True)
    return df
    
def _construct_request(raw_content, descriptor, uuid, model_name, target='TARGET'):
    message = f"""In this {descriptor}, what value falls under header "{target}"?

Do not explain, only answer.
{raw_content}
"""
    result = to_openai_batch_request(uuid, prompt_collections.table_understanding_v1, [message], model_name=model_name)
    return result

counter = 0
def create_test(df: pd.DataFrame, fmt, model_name, target='TARGET'):
    global counter
    correct_answer = df[target].iloc[0]
    idx_of_target = df.columns.get_loc(target)
    uuid = f'tabulate-{counter}-{idx_of_target}-{correct_answer}-{fmt}-{model_name}'
    counter += 1
    
    if fmt == 'csv':
        raw_content = df.to_csv(index=False)
        descriptor = 'csv table'
    elif fmt == 'csv_plus':
        raw_content = df.to_csv(index=False, sep='\t')
        raw_content = raw_content.replace('\t', ', ')
        descriptor = 'csv table' # spaced csv table
    elif fmt == 'tsv':
        raw_content = df.to_csv(index=False, sep='\t')
        descriptor = 'tsv table'
    elif fmt == 'html':
        raw_content = df.to_html(index=False)
        descriptor = 'html table'
    elif fmt == 'markdown':
        raw_content = df.to_markdown(index=False)
        descriptor = 'markdown table'
    elif fmt == 'latex':
        raw_content = df.to_latex(index=False)
        descriptor = 'latex table'
    elif fmt == 'json_record': # should be perfect
        raw_content = df.to_json(orient='records')
        descriptor = 'json record'
    elif fmt == 'control':
        # control case: space-separated
        raw_content = df.to_csv(index=False, sep=' ')
        descriptor = 'space-separated table'
        
    else:
        raise ValueError(f"Unknown format: {fmt}")
    
    return _construct_request(raw_content, descriptor, uuid, model_name, target=target)
        
    
    

if __name__ == "__main__":
    
    models = ['gpt-4o', 'gpt-4o-mini'] # , 'gpt-4-turbo', 'gpt-3.5-turbo']
    
    decks = create_decks(50, num_cols=20) # 10 repeats
    
    # deck = decks[0]
    # df = designate_target(deck, 5)
    # print(df)
    # print(df['TARGET'].iloc[0])
    # exit(0)
    
    # openai does not allow heterogeneous models
    results_by_model = {model: [] for model in models}
    for headerno in range(20):
        for deckno, deck in enumerate(decks):
            df = designate_target(deck, headerno, target='!')
            # okay good, now try a variety of formats
            # for fmt in ['csv', 'csv_plus', 'tsv', 'html', 'markdown', 'latex', 'json_record']:
            # for fmt in ['csv_plus', 'html', 'markdown', 'latex', 'control']:
            for fmt in ['html', 'markdown', 'latex']:

                for model_name in models:
                    results_by_model[model_name].append(create_test(df, fmt, model_name, target='!'))
    # print(len(results))
    # save to ./batches/batch_n.jsonl
    import os
    root = 'batches'
    os.makedirs(root, exist_ok=True)
    
    # get the latest file
    import glob
    files = glob.glob(f'{root}/batch_*.jsonl')
    if len(files) == 0:
        latest = 0
    else:
        latest = max([int(f.split('_')[-1].split('.')[0]) for f in files])
    latest += 1
    
    import json
    for model_name, results in results_by_model.items():
        with open(f'{root}/batch_{model_name}_{latest}.jsonl', 'w') as f:
            for result in results:
                f.write(json.dumps(result) + '\n')
    print(f"Saved to {root}/batch_*_{latest}.jsonl")
    
            
            


    
