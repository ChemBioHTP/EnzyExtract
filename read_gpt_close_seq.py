

from kcatextract.backform.backform_utils import get_the_yamls
from kcatextract.explode.explode_auto_context import infuse_explode_results, parse_explode_message
from kcatextract.utils.construct_batch import get_batch_output, get_batch_input, locate_correct_batch, pmid_from_usual_cid
from kcatextract.fetch_sequences.get_closest_substrate import parse_substrate_synonym
import pandas as pd

# root = 'completions/explode'
og_root = 'C:/conjunct/table_eval/completions/enzy'
og_namespace = 'brenda-rekcat-tuneboth'
og_version = 2

# og_at, og_version = locate_correct_batch(og_root, og_namespace)

# print(og_at, og_version)

compl_root = 'completions/gptclose'
batch_root = 'batches/gptclose'

namespace = f'gptclose-for-{og_namespace}-{og_version}' # -{og_version}

at, version = locate_correct_batch(compl_root, namespace)

inbatch = f"gptclose-for-{og_namespace}-{og_version}_{version}.jsonl"

print(at, version)

retain_docs = {} # type: dict[str, str]
for custom_id, docs in get_batch_input(f'{batch_root}/{inbatch}'):
    pmid = pmid_from_usual_cid(custom_id)
    assert len(docs) == 1
    retain_docs[pmid] = docs[0]

builder = []
for custom_id, content, finish_reason in get_batch_output(f'{compl_root}/{at}', allow_unfinished=False):
    # print(custom_id, finish_reason)
    index = pmid_from_usual_cid(custom_id)
    
    substrate, synonym, search = parse_substrate_synonym(retain_docs[index], ai_msg=content)
    
    print("Substrate:", substrate)
    print("Synonym:", synonym)
    print("Search:", search)
    
    builder.append({
        'substrate': substrate,
        'synonym': synonym,
        'search': search
    })
    print("\n")

# create the df
df = pd.DataFrame(builder)
timestamp = pd.Timestamp.now()

# define a "good" synonym to be where the similarity > 0.85
from rapidfuzz import fuzz

# df['similarity'] = df['synonym'].apply(lambda x: fuzz.ratio(x, df['substrate'].iloc[0]) / 100)
df['similarity'] = df.apply(lambda x: fuzz.ratio(x['synonym'], x['substrate']) / 100, axis=1)
df = df[df['similarity'] > 0.85]

df.to_csv(f"fetch_sequences/synonyms/substrate/{namespace}_{version}.tsv", index=False, sep='\t')
exit(0)
explode_df = pd.concat(dfs)

orig_df = pd.read_csv(f'data/_cache_vbrenda/_cache_{og_namespace}_{og_version}.csv')
orig_df = orig_df.astype({'pmid': str})
# enrich
enriched_df = infuse_explode_results(orig_df, explode_df)
# print(explode_df)

enriched_df.to_csv(f'data/_for_sequencing/_{namespace}_{version}.csv', index=False)


    
    


