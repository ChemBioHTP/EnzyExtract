import json
import polars as pl
from enzyextract.submit.batch_utils import locate_correct_batch

batch_folder = 'batches/similarity'
compl_folder = 'completions/similarity'
namespace = 'beluga-t2neboth-essim-brenda-4ostruct'
version = None


filename, version = locate_correct_batch(compl_folder, namespace, version=version) # , version=1)
print(f"Located {filename} version {version} in {compl_folder}")

data = []
# load data

with open(f'{compl_folder}/{filename}', 'r') as f:
    for i, line in enumerate(f):
        # data.append(json.loads(line))
        req = json.loads(line)
        items = json.loads(req['response']['body']['choices'][0]['message']['content'])['items']

        cid = req['custom_id']
        for item in items:
            # item['gpt_batch_idx'] = i
            item['custom_id'] = cid
            item['is_substrate'] = cid.endswith('s')
        data.extend(items)

input_data = []
with open(f'{batch_folder}/{filename}', 'r') as f:
    for i, line in enumerate(f):
        req = json.loads(line)
        content = req['body']['messages'][1]['content']

        cid = req['custom_id']
        lines = content.split('\n')

        # parse the regex: (\d+)\. (.*) \| (.*)$
        items = []
        for line in lines:
            if not line:
                continue
            
            idx, rest = line.split('. ', 1)

            parts = rest.split('|', 2)
            if len(parts) != 2:
                print(f"Error: {line}")
                continue
            items.append({
                'a_input': parts[0].strip(),
                'b_input': parts[1].strip(),
                'custom_id': cid,
                # 'is_substrate': cid.endswith('s'),
                'item_number': int(idx)
            })
        input_data.extend(items)

df_in = pl.DataFrame(input_data)
# print(data[0]['items'][3])
df_out = pl.DataFrame(data)

# perform a join on (custom_id, item_number)
df = df_out.join(df_in, on=['custom_id', 'item_number'], how='inner')



# fix confidences: if are_equivalent is False and confidence > 0.5, confidence should be 1 - confidence
# df = df.with_columns([
#     pl.when(~pl.col('are_equivalent') & (pl.col('confidence') > 0.5))
#         .then(1 - pl.col('confidence'))
#         .otherwise(pl.col('confidence'))
#     .alias('confidence')
# ])

df = df.select([
    'custom_id',
    'item_number',
    'a_input',
    'a_full_name',
    'b_input',
    'b_full_name',
    'are_equivalent',
    'confidence',
    'is_substrate'
])

print(df)


df.write_parquet(f'data/thesaurus/es/{namespace}_{version}.parquet')