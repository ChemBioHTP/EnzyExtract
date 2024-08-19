import pandas as pd

from kcatextract.backform.backform_utils import split_checkpoint


# def construct_context(checkpoint_df: pd.DataFrame):
#     """expected format:
#     enzymes:
#         - full name: <brenda> "catalase"
#         synonyms: "cat-1"
#         mutants: "R190Q, R203Q"
#         organisms: "Escherichia coli"
#     substrates: 
#         - full name: <brenda>
#         synonyms: ""
#     """
#     df_br, df_desc = split_checkpoint(checkpoint_df)
    
    