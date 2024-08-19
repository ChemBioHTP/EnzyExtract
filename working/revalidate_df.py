# sanity check
import pandas as pd

checkpoint_df = pd.read_csv("backform/checkpoints/rekcat_checkpoint_3 - LATEST_rekcat-vs-brenda_5.csv")
target_pmids = [10029307, 10190977, 10320327, 10427036, 10433689, 10438489, 10446163, 10473548, 10480865, 
10480878, 10480915, 10514486, 10529247, 10531334, 10564758, 10684618, 10748206, 10762259, 10801893, 10882170, 
8626758, 9398292, 9495750, 9521731, 9556600, 9576908, 9628739, 9636048, 9733678, 9933602]

# suspicious pmids: 10441376, 10714990, 8807052
target_pmids = [str(x) for x in target_pmids]


target_df = checkpoint_df[checkpoint_df['pmid'].isin(target_pmids)]
# make sure that all of these are present: enzyme, substrate, enzyme_2, substrate_2
target_df = target_df.dropna(subset=['enzyme', 'substrate', 'enzyme_2', 'substrate_2'])
# also make sure that either km or kcat is present
target_df = target_df.dropna(subset=['km', 'kcat'], how='all')
target_df = target_df.dropna(subset=['km_2', 'kcat_2'], how='all')

# make sure that each km corroborates each other
target_df = target_df.dropna(subset=['km', 'km_2'])

from hungarian.hungarian_matching import feedback_for_match
for i, row in target_df.iterrows():
    a = row['km']
    # a = "500 mM"
    b = row['km_2']
    out = feedback_for_match(a, b, 'km')
    if out and not out.startswith('off by'):
        print(out)
        print(row['pmid'])
        print(row['enzyme'], row['substrate'])
        print(row['enzyme_2'], row['substrate_2'])
        print()
    