import os
from tqdm import tqdm
import json
import polars as pl

def main():
    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    working = 'revision-prod'
    do_final_answer = False
    if working == 'apogee':
        walk_from = 'completions/enzy/apogee'
        prefix = None
        conventional_levels = True
    elif working == 'beluga':
        walk_from = 'completions/enzy'
        prefix = 'beluga'
        conventional_levels = False
    elif working == 'cherry-dev':
        walk_from = 'completions/enzy'
        prefix = 'cherry-dev'
        conventional_levels = False
    elif working == 'revision-dev':
        walk_from = 'completions/revision'
        prefix = 'tablevision-dev'
        conventional_levels = False
        # do_final_answer = True
    elif working == 'revision-prod':
        walk_from = 'completions/revision'
        prefix = 'tablevision-prod'
        conventional_levels = False
        # do_final_answer = True

    # read every jsonl in completions/enzy/apogee as long as it doesn't end in 0.jsonl
    filepaths = []
    for dirpath, dirnames, filenames in os.walk(walk_from):
        for filename in filenames:
            if prefix and not filename.startswith(prefix):
                continue
            # if filename.endswith('0.jsonl'):
                # continue
            if filename.endswith('.429.jsonl'):
                continue
            if not filename.endswith('.jsonl'):
                continue
            filepaths.append(os.path.join(dirpath, filename))

    # collect what we want
    collected = []
    for filepath in tqdm(filepaths):

        # print("Reading", filename)
        with open(filepath, 'r') as f:
            for line in f:
                req = json.loads(line)
                content = req['response']['body']['choices'][0]['message']['content']
                stop_reason = req['response']['body']['choices'][0]['finish_reason']
                # if stop_reason != 'stop':
                cid = req['custom_id']
                if conventional_levels:
                    toplevel, secondlevel, _ = cid.split('-', 2)
                else:
                    toplevel = None
                    secondlevel = None
                
                final_answer = None
                if do_final_answer:
                    try:
                        obj = json.loads(content, strict=False)
                        final_answer = obj['final_answer']
                    except:
                        pass
                pmid = cid.split('_', 2)[2]
                collected.append((cid, toplevel, secondlevel, pmid, content, final_answer, stop_reason))
    
    df = pl.DataFrame(collected, schema=['custom_id', 'toplevel', 'secondlevel', 'pmid', 'content', 'final_answer', 'stop_reason'], orient='row', schema_overrides={
        'custom_id': pl.Utf8,
        'toplevel': pl.Utf8,
        'secondlevel': pl.Utf8,
        'pmid': pl.Utf8,
        'content': pl.Utf8,
        'final_answer': pl.Utf8,
        'stop_reason': pl.Utf8
    })
    # print(df)
    df = df.with_columns([
        pl.col("content").str.contains("```").alias("has_yaml"),
    ])
    df.write_parquet(f'data/gpt/{working}_gpt.parquet')

if __name__ == '__main__':
    main()