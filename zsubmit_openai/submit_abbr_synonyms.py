# working_enzy_table_md, but tableless

import polars as pl

from enzyextract.utils.construct_batch import chunked_write_to_jsonl
from enzyextract.utils.fresh_version import next_available_version
from enzyextract.utils.openai_management import process_env, submit_batch_file
from enzyextract.utils.openai_schema import to_openai_batch_request_with_schema
from enzyextract.utils.namespace_management import glean_model_name
from enzyextract.prompts import for_abbreviations

namespace = 'beluga-abbr-4ostruct'
dest_folder = 'batches/synonyms/abbr'

# version = next_available_version(dest_folder, namespace, '.jsonl')
version = '20241213'
print("Namespace: ", namespace)
print("Using version: ", version)


prompt_enzyme = for_abbreviations.abbr_v0_0_0
prompt_schema = for_abbreviations.AbbreviationsSchema

model_name, _, structured = glean_model_name(namespace)

print("Using model", model_name)


# load data
df = pl.read_parquet('data/synonyms/abbr/beluga_paper_abbrs.parquet')
df = df.drop_nulls('content')

df = df.group_by('pmid').agg('content')

df = df.with_columns([
    pl.col('content').list.join('\n').alias('content')
])
# get pairs of (enzyme_preferred, enzyme_preferred_2) and (substrate_preferred, substrate_preferred_2)


batch = []
# chunks of 10 from enzyme_translations
for pmid, content in df.iter_rows():
    docs = [content]
    req = to_openai_batch_request_with_schema(f'''{namespace}_{version}_{pmid}''', prompt_enzyme, docs, 
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