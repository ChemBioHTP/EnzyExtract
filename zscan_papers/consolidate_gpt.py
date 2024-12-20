import os
from tqdm import tqdm
import json
import polars as pl

def main():
    # read every jsonl in completions/enzy/apogee as long as it doesn't end in 0.jsonl
    filepaths = []
    for dirpath, dirnames, filenames in os.walk('completions/enzy'):
        for filename in filenames:
            if not filename.startswith('beluga'):
                continue
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
                content = req['response']['body']['choices'][0]['message']['content']
                stop_reason = req['response']['body']['choices'][0]['finish_reason']
                # if stop_reason != 'stop':
                cid = req['custom_id']
                # toplevel, secondlevel, _ = cid.split('-', 2)
                toplevel = None
                secondlevel = None
                pmid = cid.split('_', 2)[2]
                collected.append((cid, toplevel, secondlevel, pmid, content, stop_reason))
    
    df = pl.DataFrame(collected, schema=['custom_id', 'toplevel', 'secondlevel', 'pmid', 'content', 'stop_reason'], orient='row', schema_overrides={
        'custom_id': pl.Utf8,
        'toplevel': pl.Utf8,
        'secondlevel': pl.Utf8,
        'pmid': pl.Utf8,
        'content': pl.Utf8,
        'stop_reason': pl.Utf8
    })
    # print(df)
    df = df.with_columns([
        pl.col("content").str.contains("```").alias("has_yaml"),
    ])
    df.write_parquet('data/gpt/beluga_gpt.parquet')

if __name__ == '__main__':
    main()