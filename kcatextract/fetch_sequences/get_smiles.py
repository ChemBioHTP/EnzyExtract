### SCRIPT 1: RDKit

import os
import time
from urllib.request import urlopen
from urllib.parse import quote

import pandas as pd
import requests

from tqdm import tqdm

def CIRconvert(ids):
    # https://cactus.nci.nih.gov/chemical/structure
    # https://cactus.nci.nih.gov/chemical/structure_documentation
    try:
        url = 'http://cactus.nci.nih.gov/chemical/structure/' + quote(ids) + '/smiles'
        ans = urlopen(url).read().decode('utf8')
        return ans
    except:
        return 'not found'



def rdkit_main(identifiers, wait=0.25):
    # for ids in identifiers:
    # batches of 10
    result = []
    # for i in range(0, len(identifiers), 10):
        # batch = identifiers[i:i+10]
        # for ids in batch:
            # print(ids, CIRconvert(ids))
    for x in identifiers:
        smiles = CIRconvert(x)
        result.append({'Name': x, 'Smiles': smiles})
        time.sleep(wait)
    df = pd.DataFrame(result)
    return df

def rdkit_inchi(names, inchis, wait=0.25):
    result = []
    for name, inchi in tqdm(zip(names, inchis), total=len(names)):
        smiles = CIRconvert(inchi)
        result.append({'Name': name, 'InChI': inchi, 'Smiles': smiles})
        time.sleep(wait)
    df = pd.DataFrame(result)
    return df

def pubchem_main(identifiers, wait=0.25):
    # get synonyms:
    # https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/aspirin/synonyms/TXT
    result = [] # pd.DataFrame(columns = ['Name', 'Smiles'])
    fails = 0
    for x in identifiers:
        try:
            url = 'https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/' + x + '/property/CanonicalSMILES/TXT'
    #         remove new line character with rstrip
            smiles = requests.get(url).text.rstrip().replace('\n', '')
            if('NotFound' in smiles):
                result.append((x, "not found"))
            else: 
                result.append((x, smiles))
        except Exception as e: 
            fails += 1
            print(e)
            print("boo", x)
            if fails > 5:
                break
        time.sleep(wait)
    df = pd.DataFrame(result, columns = ['Name', 'Smiles'])
    return df


brenda_inchi_all_df = None
def convert_through_inchi(identifiers):
    global brenda_inchi_all_df
    if brenda_inchi_all_df is None:
        brenda_inchi_all_df = pd.read_csv('fetch_sequences/results/smiles/brenda_inchi_all.tsv', sep='\t')
    subset = brenda_inchi_all_df[brenda_inchi_all_df['name'].isin(set(identifiers))]
    
    return rdkit_inchi(subset['name'], subset['inchi'])
    

def script0():
    # identifiers = ['3-Methylheptane', 'Aspirin', 'Diethylsulfate', 'Diethyl sulfate', '50-78-2', 'Adamant']
    # identifiers = ['p-NPP'] # not found
    identifiers = ['pNPP']
    rdkit = rdkit_main(identifiers)
    pubchem = pubchem_main(identifiers)
    print(rdkit)
    print(pubchem)
    exit(0)

if __name__ == "__main__":
    script0()
    
    # rdkit_main(identifiers)
    # df = pubchem_main(identifiers)
    # print(df)
    
    # brenda_main(identifiers)
    
    src_df = pd.read_csv('completions/enzy/rekcat-vs-brenda_6.csv')
    # NB: remove nan
    # Note: converting to lower does not change total number (1187)
    
    identifiers = src_df['substrate_2'].dropna().unique()
    
    # print(identifiers)
    # print(len(identifiers))
    from tqdm import tqdm
    
    pubchem_path = 'fetch_sequences/results/smiles/pubchem_smiles.csv'
    if not os.path.exists(pubchem_path):
        pubchem_df = pubchem_main(tqdm(identifiers))
        pubchem_df.to_csv(pubchem_path, index=False)
    else:
        pubchem_df = pd.read_csv(pubchem_path)
    
    # get remaining idents
    identifiers = set(pubchem_df[pubchem_df['Smiles'] == 'not found']['Name'].values)
    
    rdkit_path = 'fetch_sequences/results/smiles/rdkit_smiles.csv'
    if not os.path.exists(rdkit_path):
        rdkit_df = rdkit_main(tqdm(identifiers))
        rdkit_df.to_csv(rdkit_path, index=False)
    else:
        rdkit_df = pd.read_csv(rdkit_path)
    
    # get remaining idents
    brenda_path = 'fetch_sequences/results/smiles/brenda_inchi.csv'
    # if not os.path.exists(brenda_path):
    remaining = sorted(list(set(rdkit_df[rdkit_df['Smiles'] == 'not found']['Name'].values)))
    brenda_df = convert_through_inchi(remaining)
    brenda_df.to_csv('fetch_sequences/results/smiles/brenda_inchi.csv', index=False)
    


    
    
