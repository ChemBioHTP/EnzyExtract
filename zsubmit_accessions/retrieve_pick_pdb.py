import polars as pl
import os
from tqdm import tqdm
import json
import re

def accept_filename(x: str):
    return x.endswith('.jsonl') and 'pdb-prod2' in x

def load_ingest():
    # get_stuff = re.compile('Target Enzyme:/ (.*)\nTarget Fullname: (.*)\n(Target Organism: (.*)\n)?')

    collected = []
    for filename in tqdm(os.listdir('batches/pick')):
        if not accept_filename(filename):
            continue
        filepath = f'batches/pick/{filename}'
        with open(filepath, 'r') as f:
            for line in f:
                req = json.loads(line)
                prompt = req['body']['messages'][0]['content']
                content = ''
                for msg in req['body']['messages'][1:]:
                    content += msg['content'] + '\n'
                
                # gps = 
                # stuff_match = get_stuff.match(content)
                # enzyme, enzyme_full, _, organism = stuff_match.groups()
                assert 'Target Enzyme: ' in content
                enzyme, rest = content.split('Target Enzyme: ', 1)[1].split('\n', 1)
                enzyme_full = None
                organism = None
                if 'Target Fullname: ' in rest:
                    enzyme_full, rest = rest.split('Target Fullname: ', 1)[1].split('\n', 1)
                if 'Target Organism: ' in rest:
                    organism, rest = rest.split('Target Organism: ', 1)[1].split('\n', 1)


                # then, extract everything that comes after
                acc = []
                for line in rest.split('\n'):
                    if not line:
                        continue
                    if ': ' in line:
                        key, value = line.split(': ', 1)
                        acc.append(key)
                
                if enzyme_full == 'None':
                    enzyme_full = None
                cid = req['custom_id']
                _, idx, pmid = cid.split('_', 2)
                collected.append((# cid, 
                                  idx, pmid, content, acc,
                    enzyme, enzyme_full, organism))
    df = pl.DataFrame(collected, 
        orient='row',
        schema=[# 'custom_id', 
                'idx', 'pmid', 'content', 'tried_accessions',
            'enzyme', 'enzyme_full', 'organism'],
        schema_overrides={
            # 'custom_id': pl.Utf8,
            'idx': pl.UInt32,
            'pmid': pl.Utf8,
            'tried_accessions': pl.List(pl.Utf8),
            # 'content': pl.Utf8,
            'enzyme': pl.Utf8,
            'enzyme_full': pl.Utf8,
            'organism': pl.Utf8
        })
    return df


def load_gpt():
    collected = []
    for filename in tqdm(os.listdir('completions/pick')):
        if not accept_filename(filename):
            continue
        filepath = f'completions/pick/{filename}'
        with open(filepath, 'r') as f:
            for line in f:
                req = json.loads(line)
                content = req['response']['body']['choices'][0]['message']['content']
                stop_reason = req['response']['body']['choices'][0]['finish_reason']
                # if stop_reason != 'stop':
                cid = req['custom_id']
                
                _, idx, pmid = cid.split('_', 2)
                
                try:
                    obj = json.loads(content, strict=False)
                    thoughts = obj['thoughts_and_comments']
                    best = obj['best']
                    second = obj['second_best']
                    third = obj['third_best']
                    # additional = obj['additional']
                except:
                    pass
                pmid = cid.split('_', 2)[2]
                collected.append((# cid, 
                                  idx, pmid, # content, 
                    thoughts, best, second, third, # additional, 
                    stop_reason))

    gpt_df = pl.DataFrame(collected, 
        orient='row',
        schema=[# 'custom_id', 
                'idx', 'pmid', # 'content', 
            'thoughts', 'best', 'second', 'third', # 'additional', 
            'stop_reason'],
        schema_overrides={
            # 'custom_id': pl.Utf8,
            'idx': pl.UInt32,
            'pmid': pl.Utf8,
            # 'content': pl.Utf8,
            'thoughts': pl.Utf8,
            'best': pl.Utf8,
            'second': pl.Utf8,
            'third': pl.Utf8,
            # 'additional': pl.List(pl.Utf8),
            'stop_reason': pl.Utf8
        })
    return gpt_df

ingest_df = load_ingest()

gpt_df = load_gpt()
df = ingest_df.join(gpt_df, on=['idx', 'pmid'], how='inner')
print(df)

# df.select('pmid', 'enzyme', 'enzyme_full', 'organism', 'best')
df = df.rename({'best': 'pdb'})
df.write_parquet('data/thesaurus/enzymes/pdb_picked.parquet')

# print(gpt_df)