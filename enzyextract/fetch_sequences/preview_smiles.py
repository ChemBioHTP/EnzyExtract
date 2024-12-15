import pandas as pd


df = pd.read_csv('fetch_sequences/results/smiles/pubchem_smiles_og.csv')

# replace all \n with nothing
df['Smiles'] = df['Smiles'].str.replace('\n', '')
# print(df[df['Name'] == 'maltose'])
# save to csv
df.to_csv('fetch_sequences/results/smiles/pubchem_smiles.csv', index=False)