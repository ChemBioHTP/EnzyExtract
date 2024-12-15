from enzyextract.thesaurus.convert_substrates import latest_smiles_df, latest_inchi_df
from datetime import datetime

df = latest_smiles_df()
# print(df)


df2 = latest_inchi_df()
print(df2)