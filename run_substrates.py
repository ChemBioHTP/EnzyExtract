import pandas as pd
from kcatextract.fetch_sequences.get_closest_substrate import infuse_with_substrates

def script0():
    # given a validation df, obtain substrates
    valid_df = pd.read_csv("data/_compiled/nonbrenda.tsv", sep='\t')
    brenda_substrate_df = pd.read_csv("fetch_sequences/results/smiles/brenda_inchi_all.tsv", sep="\t")
    valid_with_seq = infuse_with_substrates(valid_df, brenda_substrate_df, redo_rdkit=True)
    valid_with_seq.to_csv("data/_compiled/nonbrenda_with_substrate_2.tsv", sep='\t', index=False)

if __name__ == '__main__':
    script0()