import os
from tqdm import tqdm
import json
import polars as pl

def main():
    # read every jsonl in completions/enzy/apogee as long as it doesn't end in 0.jsonl
    filepaths = []
    for dirpath, dirnames, filenames in os.walk('batches/enzy'):
        for filename in filenames:
            # if not filename.startswith('beluga'):
            #     continue
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
                content = ''
                for msg in req['body']['messages']:
                    content += msg['content'] + '\n'
                # if stop_reason != 'stop':
                cid = req['custom_id']
                toplevel, secondlevel, _ = cid.split('-', 2)
                # toplevel = None
                # secondlevel = None
                pmid = cid.split('_', 2)[2]
                collected.append((cid, toplevel, secondlevel, pmid, content))
    
    df = pl.DataFrame(collected, schema=['custom_id', 'toplevel', 'secondlevel', 'pmid', 'content'], orient='row', schema_overrides={
        'custom_id': pl.Utf8,
        'toplevel': pl.Utf8,
        'secondlevel': pl.Utf8,
        'pmid': pl.Utf8,
        'content': pl.Utf8,
        # 'stop_reason': pl.Utf8
    })
    # print(df)
    df.write_parquet('data/ingest/beluga_ingest.parquet')

if __name__ == '__main__':
    main()