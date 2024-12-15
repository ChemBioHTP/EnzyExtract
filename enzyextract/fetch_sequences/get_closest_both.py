
import pandas as pd

from enzyextract.fetch_sequences import get_closest_enzyme
from enzyextract.fetch_sequences.get_closest_substrate import infuse_with_substrates


def infuse_enzyme_substrate_sequences(checkpoint_df, dists_df, brenda_df, brenda_substrate_df, redo_inchi=False):
    # run it all
    
    # step 1
    sequence_df = get_closest_enzyme.to_sequence_df(checkpoint_df, brenda_df)
    
    # step 2,3 anoint sequence_df with info via dists_df
    get_closest_enzyme.join_distances(sequence_df, dists_df)
    
    # step 4, join checkpoint_df with sequence_df
    get_closest_enzyme.join_sequence_df(checkpoint_df, sequence_df)
    
    infuse_with_substrates(checkpoint_df, brenda_substrate_df, redo_inchi_convert=redo_inchi)
    
    return checkpoint_df
    
    
    # print(checkpoint_df)

if __name__ == '__main__':
    
    checkpoint_df = pd.read_csv("_debug/_cache_vbrenda/_cache_brenda-rekcat-tuneboth_2.csv") # 95/340
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    brenda_df = pd.read_csv("fetch_sequences/enzymes/brenda_enzymes.tsv", sep="\t")
    brenda_substrate_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")
    
    out = infuse_enzyme_substrate_sequences()
    out.to_csv(None, None, None, "_debug/_cache_seq/_cache_brenda-rekcat-tuneboth_2.tsv", index=False, sep="\t")