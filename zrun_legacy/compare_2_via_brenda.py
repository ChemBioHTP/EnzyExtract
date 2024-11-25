import os
import pandas as pd

from compare_brenda import run_stats
from kcatextract.utils.construct_batch import locate_correct_batch
from kcatextract.metrics.polaric import precision_recall, mean_log_relative_ratio
blacklist = whitelist = None

namespace_a = 'openelse-brenda-md-4o'
version_a = 1
namespace_b = 'openelse-brenda-md-t4neboth'
version_b = 1

whitelist_df = pd.read_csv(f'data/mbrenda/_cache_{namespace_a}_{version_a}.csv', dtype={'pmid': str})
whitelist = set(whitelist_df['pmid'])
whitelist_df = pd.read_csv(f'data/mbrenda/_cache_{namespace_b}_{version_b}.csv', dtype={'pmid': str})
whitelist &= set(whitelist_df['pmid'])

for namespace in [namespace_a, namespace_b]:


    structured = namespace.endswith('-str')
    version = None
    # compl_folder = 'C:/conjunct/table_eval/completions/enzy'
    compl_folder = 'completions/enzy'

    # merge fragments
    # check to see if we need to merge
    need_merge = False
    filename, version = locate_correct_batch(compl_folder, namespace)
    if filename.endswith('0.jsonl'):
        need_merge = True

    if need_merge:
        from kcatextract.utils.openai_management import merge_chunked_completions
        print(f"Merging all chunked completions for {filename} v{version}. Confirm? (y/n)")    
        if input() != 'y':
            exit(0)
        merge_chunked_completions(namespace, version=version, compl_folder=compl_folder, dest_folder=compl_folder)

    stats = run_stats(namespace=namespace, version=version, compl_folder=compl_folder, blacklist=blacklist, whitelist=whitelist,
                against_brenda=True,
                silence=False,
                use_yaml=not structured)

    # append stats to block_stats.tsv
    dest = 'data/block_stats.tsv'
    df = pd.DataFrame([stats])

    if not os.path.exists(dest):
        df.to_csv(dest, index=False, sep='\t')
    else:
        df.to_csv('data/block_stats.tsv', mode='a', header=False, index=False, sep='\t')
    
