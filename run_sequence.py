import pandas as pd

from kcatextract.fetch_sequences.get_closest_both import infuse_enzyme_substrate_sequences

def script0():
    
    namespace = "explode-for-brenda-rekcat-tuneboth-2"
    version = 2
    checkpoint_df = pd.read_csv(f"data/_for_sequencing/_{namespace}_{version}.csv")
    # fetch_sequences symlink
    dists_df = pd.read_csv("fetch_sequences/enzymes/rekcat_mutant_distances_gpt.tsv", sep="\t")
    brenda_df = pd.read_csv("fetch_sequences/enzymes/brenda_enzymes.tsv", sep="\t")
    brenda_substrate_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")

    out = infuse_enzyme_substrate_sequences(
                    checkpoint_df=checkpoint_df, dists_df=dists_df, brenda_df=brenda_df, 
                    brenda_substrate_df=brenda_substrate_df, redo_inchi=True)
    
    assert len(out) == len(checkpoint_df)
    
    out.to_csv(f"data/_post_sequencing/_{namespace}_{version}.tsv", sep="\t", index=False)

if __name__ == '__main__':
    script0()