

from kcatextract.backform.backform_utils import get_the_yamls
from kcatextract.explode.explode_auto_context import infuse_explode_results, parse_explode_message
from kcatextract.utils.construct_batch import get_batch_output, locate_correct_batch, pmid_from_usual_cid
import pandas as pd

# root = 'completions/explode'
og_root = 'C:/conjunct/table_eval/completions/enzy'
og_namespace = 'brenda-rekcat-tuneboth'
og_version = 2

# og_at, og_version = locate_correct_batch(og_root, og_namespace)

# print(og_at, og_version)

root = 'completions/explode'

namespace = f'explode-for-{og_namespace}-{og_version}' # -{og_version}

at, version = locate_correct_batch(root, namespace)

print(at, version)

dfs = []
for custom_id, content, finish_reason in get_batch_output(f'{root}/{at}'):
    # print(custom_id, finish_reason)
    pmid = pmid_from_usual_cid(custom_id)
    if finish_reason == 'length':
        print("Too long:", pmid)
        continue
    df, is_valid = parse_explode_message(content)
    if not is_valid:
        print("Invalid:", pmid)
        continue
    df['pmid'] = pmid
    dfs.append(df)

explode_df = pd.concat(dfs)

orig_df = pd.read_csv(f'data/_cache_vbrenda/_cache_{og_namespace}_{og_version}.csv')
orig_df = orig_df.astype({'pmid': str})
# enrich
enriched_df = infuse_explode_results(orig_df, explode_df)
# print(explode_df)

enriched_df.to_csv(f'data/_for_sequencing/_{namespace}_{version}.csv', index=False)


    
    


