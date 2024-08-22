
import pandas as pd

from kcatextract.fetch_sequences import get_closest_enzyme
from kcatextract.fetch_sequences.get_closest_substrate import infuse_with_substrates


def script0():
    # run it all
    checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_brenda-rekcat-tuneboth_2.csv") # 95/340
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    
    brenda_df = pd.read_csv("fetch_sequences/enzymes/brenda_enzymes.tsv", sep="\t")
    
    # step 1
    sequence_df = get_closest_enzyme.to_sequence_df(checkpoint_df, brenda_df)
    
    # step 2,3 anoint sequence_df with info via dists_df
    get_closest_enzyme.join_distances(sequence_df, dists_df)
    
    # step 4, join checkpoint_df with sequence_df
    get_closest_enzyme.join_sequence_df(checkpoint_df, sequence_df)
    
    infuse_with_substrates(checkpoint_df, redo_inchi=True)
    
    checkpoint_df.to_csv("_debug/_cache_seq/_cache_brenda-rekcat-tuneboth_2.tsv", index=False, sep="\t")
    
    print(checkpoint_df)

if __name__ == '__main__':
    script0()