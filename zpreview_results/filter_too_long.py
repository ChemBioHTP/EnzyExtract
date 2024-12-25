# filter those completions where GPT was cut off
import polars as pl
import os
import json
from tqdm import tqdm
def main():
    # read every jsonl in completions/enzy/apogee as long as it doesn't end in 0.jsonl
    filepaths = []
    for dirpath, dirnames, filenames in os.walk('completions/enzy/apogee'):
        for filename in filenames:
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
                if stop_reason != 'stop':
                    collected.append((req['custom_id'], content, stop_reason))
    
    df = pl.DataFrame(collected, schema=['custom_id', 'content', 'stop_reason'], orient='row', schema_overrides={
        'custom_id': pl.Utf8,
        'content': pl.Utf8,
        'stop_reason': pl.Utf8
    })
    print(df)
    df.write_parquet('data/gpt/apogee_gpt_cut_off.parquet')

if __name__ == '__main__':
    main()