import json
import polars as pl
from enzyextract.utils.construct_batch import locate_correct_batch

batch_folder = 'batches/synonyms/abbr'
compl_folder = 'completions/synonyms/abbr'
namespace = 'beluga-abbr-4ostruct'
version = None


filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
print(f"Located {filename} version {version} in {compl_folder}")

data = []


# load data
with open(f'{compl_folder}/{filename}', 'r') as f:
    for i, line in enumerate(f):
        # data.append(json.loads(line))
        req = json.loads(line)
        content = req['response']['body']['choices'][0]['message']['content']
        try:
            items = json.loads(content, strict=False)['items']
        except Exception as e:
            print(f"Error on line {i}")
            print(content)
            print(e)
            continue

        cid = req['custom_id']
        pmid = cid.split('_', 2)[-1]
        for item in items:
            # item['gpt_batch_idx'] = i
            item['custom_id'] = cid
            item['pmid'] = pmid
            # item['is_substrate'] = cid.endswith('s')
        data.extend(items)

# load input data
input_data = []
with open(f'{batch_folder}/{filename}', 'r') as f:
    for i, line in enumerate(f):
        req = json.loads(line)
        content = req['body']['messages'][1]['content']

        cid = req['custom_id']
        lines = content.split('\n')
        # don't need this


# df_in = pl.DataFrame(input_data)
df = df_out = pl.DataFrame(data)

# perform a join on (custom_id, item_number)
# df = df_out.join(df_in, on=['custom_id', 'item_number'], how='inner')

print(df)


df.write_parquet(f'data/synonyms/abbr/{namespace}_{version}.parquet')