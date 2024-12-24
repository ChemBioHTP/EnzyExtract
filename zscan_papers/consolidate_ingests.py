import os
from tqdm import tqdm
import json
import polars as pl

def main():
    # read every jsonl in completions/enzy/apogee as long as it doesn't end in 0.jsonl

    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    working = 'revision-dev'

    if working == 'apogee':
        walk_from = 'batches/enzy/apogee'
        prefix = None
        conventional_levels = True
    elif working == 'beluga':
        walk_from = 'batches/enzy'
        prefix = 'beluga'
        conventional_levels = False
    elif working == 'cherry-dev':
        walk_from = 'batches/enzy'
        prefix = 'cherry-dev'
        conventional_levels = False
    elif working == 'revision-dev':
        walk_from = 'batches/revision'
        prefix = 'tablevision-dev'
        conventional_levels = False

    filepaths = []
    for dirpath, dirnames, filenames in os.walk(walk_from):
        for filename in filenames:
            if prefix and not filename.startswith(prefix):
                continue
            # ingests do not get merged
            if filename.endswith('0.jsonl'):
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
                prompt = req['body']['messages'][0]['content']
                content = ''
                for msg in req['body']['messages'][1:]:
                    content += msg['content'] + '\n'
                # pages = [x['content'] for x in req['body']['messages'][1:]]
                # if stop_reason != 'stop':
                cid = req['custom_id']
                if conventional_levels:
                    toplevel, secondlevel, _ = cid.split('-', 2)
                else:
                    toplevel = None
                    secondlevel = None
                pmid = cid.split('_', 2)[2]
                collected.append((cid, toplevel, secondlevel, pmid, content, prompt))
    
    df = pl.DataFrame(collected, schema=['custom_id', 'toplevel', 'secondlevel', 'pmid', 'content', 
                                         'prompt', 
                                        #  'pages'
                                         ], orient='row', schema_overrides={
        'custom_id': pl.Utf8,
        'toplevel': pl.Utf8,
        'secondlevel': pl.Utf8,
        'pmid': pl.Utf8,
        'content': pl.Utf8,
        'prompt': pl.Utf8,
        # 'pages': pl.List(pl.Utf8)
        # 'stop_reason': pl.Utf8
    })
    # print(df)
    df.write_parquet(f'data/ingest/{working}_ingest.parquet')

if __name__ == '__main__':
    main()