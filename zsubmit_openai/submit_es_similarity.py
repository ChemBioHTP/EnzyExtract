# working_enzy_table_md, but tableless

import polars as pl

from enzyextract.utils.construct_batch import chunked_write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.openai_management import process_env, submit_batch_file
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema
from enzyextract.utils.namespace_management import glean_model_name
from enzyextract.prompts import agentic_similarity

namespace = 'beluga-t2neboth-essim-brenda-4ostruct'
dest_folder = 'batches/similarity'

version = next_available_version(dest_folder, namespace, '.jsonl')
print("Namespace: ", namespace)
print("Using version: ", version)


prompt_enzyme = agentic_similarity.enzyme_substrate_v0_0_0(es='enzyme')
prompt_substrate = agentic_similarity.enzyme_substrate_v0_0_0(es='substrate')
prompt_schema = agentic_similarity.ESSimilaritySchema

model_name, _, structured = glean_model_name(namespace)

print("Using model", model_name)


# load data
df = pl.read_parquet('data/humaneval/comparisons/rich/rich_beluga-t2neboth_brenda.parquet')
# get pairs of (enzyme_preferred, enzyme_preferred_2) and (substrate_preferred, substrate_preferred_2)

if 'enzyme_preferred' not in df.columns:
    df = df.with_columns([
        (pl.coalesce(['enzyme_full', 'enzyme']) if 'enzyme_full' in df.columns else pl.col('enzyme'))
            .alias('enzyme_preferred'),
    ])
if 'enzyme_preferred_2' not in df.columns:
    df = df.with_columns([
        (pl.coalesce(['enzyme_full_2', 'enzyme_2']) if 'enzyme_full_2' in df.columns else pl.col('enzyme_2'))
            .alias('enzyme_preferred_2'),
    ])
if 'substrate_preferred' not in df.columns:
    df = df.with_columns([
        (pl.coalesce(['substrate_full', 'substrate']) if 'substrate_full' in df.columns else pl.col('substrate'))
            .alias('substrate_preferred'), 
    ])
if 'substrate_preferred_2' not in df.columns:
    df = df.with_columns([
        (pl.coalesce(['substrate_full_2', 'substrate_2']) if 'substrate_full_2' in df.columns else pl.col('substrate_2'))
            .alias('substrate_preferred_2'),
    ])

# get unique pairs
enzyme_translations = df[['enzyme_preferred', 'enzyme_preferred_2']].unique().drop_nulls()
substrate_translations = df[['substrate_preferred', 'substrate_preferred_2']].unique().drop_nulls()


def to_doc(translation_subset: pl.DataFrame, enzyme=True):
    builder = ""
    if enzyme:
        for i, row in enumerate(translation_subset.iter_rows(named=True)):
            builder += f'''{i+1}. {row['enzyme_preferred']} | {row['enzyme_preferred_2']}\n'''
    else:
        for i, row in enumerate(translation_subset.iter_rows(named=True)):
            builder += f'''{i+1}. {row['substrate_preferred']} | {row['substrate_preferred_2']}\n'''
    return builder

batch = []
# chunks of 10 from enzyme_translations
for i in range(0, len(enzyme_translations), 10):
    chunk = enzyme_translations[i:i+10]
    docs = [to_doc(chunk)]
    req = to_openai_batch_request_with_schema(f'''{namespace}_{version}_{i}e''', prompt_enzyme, docs, 
                                  model_name=model_name, schema=prompt_schema)
    batch.append(req)

# chunks of 10 from substrate_translations
for i in range(0, len(substrate_translations), 10):
    chunk = substrate_translations[i:i+10]
    docs = [to_doc(chunk, enzyme=False)]
    req = to_openai_batch_request_with_schema(f'''{namespace}_{version}_{i}s''', prompt_substrate, docs, 
                                  model_name=model_name, schema=prompt_schema)
    batch.append(req)

print(docs[0])
    
will_write_to = f'{dest_folder}/{namespace}_{version}.jsonl'
# write in chunks

chunks = chunked_write_to_jsonl(batch, will_write_to, chunk_size=1000)
for chunk in chunks:

    try:
        batchname = submit_batch_file(chunk, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", will_write_to)
        print(e)