
import re



# Define the regex patterns for each ID type
# pdb cannot start with 0
pdb_pattern = re.compile(r'\b[1-9][A-Z0-9]{3}\b', re.IGNORECASE)
# uniprot_pattern = re.compile(r'\b[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]\b', re.IGNORECASE)
# uniprot_v2_pattern = re.compile(r'\b[OPQ][0-9][A-Z0-9]{3}[0-9]\b', re.IGNORECASE)
# uniprot_v3_pattern = re.compile(r'\b[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9][A-Z0-9]{2}[0-9]\b', re.IGNORECASE)
# OOF ^ the above is wrong!
uniprot_pattern = re.compile(r'[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2}', re.IGNORECASE)
uniprot_blacklist = ['K2HPO4', "C8H9O2", 'H2S2O3'] # K2HP04
# ncbi_pattern = re.compile(r'\b[ANWCTSG][0-9]+\b')

refseq_pattern = re.compile(r'\b((?:A[CP]|N[CGTWZMRP]|X[MRP]|YP|WP)_\d+(?:\.\d+)?)\b')

genbank_pattern = re.compile(r'\b([A-Z]{1,3}\d{5,8}(?:\.\d+)?)\b', re.IGNORECASE)

def search_ids(all_txt: str):

    # Iterate over the documents
    # all_txt = "\n".join([x.page_content for x in documents])
        # Search for PDB IDs
    pdb_matches = pdb_pattern.findall(all_txt)
    # pdbs must have at least 1 letter
    pdb_matches = [x for x in pdb_matches if any(c.isalpha() for c in x)]
    # remove duplicates and null entries
    pdb_matches = [x for x in set(pdb_matches) if x]

    # Search for UniProt IDs
    uniprot_matches = uniprot_pattern.findall(all_txt) # + uniprot_v2_pattern.findall(all_txt) + uniprot_v3_pattern.findall(all_txt)
    uniprot_matches = [x for x in set(uniprot_matches) if x and x not in uniprot_blacklist]
    # filter out known false positives
    
    # Search for NCBI accession numbers
    # ncbi_matches = ncbi_pattern.findall(all_txt)

    refseq_matches = refseq_pattern.findall(all_txt)
    refseq_matches = [x for x in set(refseq_matches) if x]
    genbank_matches = genbank_pattern.findall(all_txt)
    genbank_matches = [x for x in set(genbank_matches) if x]

    return pdb_matches, uniprot_matches, refseq_matches, genbank_matches

amino1 = "ACDEFGHIKLMNPQRSTVWY"
amino3 = "Ala|Cys|Asp|Glu|Phe|Gly|His|Ile|Lys|Leu|Met|Asn|Pro|Gln|Arg|Ser|Thr|Val|Trp|Tyr"
mutant_pattern = re.compile(r'\b([ACDEFGHIKLMNPQRSTVWY][1-9]\d{1,3}[ACDEFGHIKLMNPQRSTVWY])\b')
mutant_v2_pattern = re.compile(rf'\b(?:{amino3})[1-9]\d{{1,3}}(?:{amino3})\b', re.IGNORECASE)
mutant_v3_pattern = re.compile(rf'\b((?:{amino3})[1-9]\d{{0,3}}(?:{amino3}))\b', re.IGNORECASE)

mutant_v4_pattern = re.compile(rf'\b((?:{amino3})-?[1-9]\d{{1,3}})\b', re.IGNORECASE)
def search_mutants(all_txt: str):
    # search text for mutant codes
    mutant_matches = mutant_pattern.findall(all_txt) + mutant_v2_pattern.findall(all_txt)
    return list(set(mutant_matches))

from tqdm import tqdm
import os
import pymupdf
import pandas as pd

def search_folder_for_enzyme_idents(folder):
    result = []
    for filename in tqdm(os.listdir(folder)):
        try:
            doc = pymupdf.open(f"{folder}/{filename}")
        except Exception as e:
            print("Error opening", filename)
            print(e)
            continue
        all_txt = "\n".join([page.get_text('text') for page in doc])
        pdb_matches, uniprot_matches, refseq_matches, genbank_matches = search_ids(all_txt)
        result.append((filename, pdb_matches, uniprot_matches, refseq_matches, genbank_matches))
    df = pd.DataFrame(result, columns=['filename', 'pdb', 'uniprot', 'refseq', 'genbank'])
    return df

def search_folder_for_mutants(folder):
    result = []
    for filename in tqdm(os.listdir(folder)):
        try:
            doc = pymupdf.open(f"{folder}/{filename}")
        except Exception as e:
            print("Error opening", filename)
            print(e)
            continue
        all_txt = "\n".join([page.get_text('text') for page in doc])
        mutant_matches = search_mutants(all_txt)
        result.append((filename, '; '.join(mutant_matches)))
    df = pd.DataFrame(result, columns=['filename', 'mutants'])
    return df

# def script0():
    
#     dest_df_path = "fetch_sequences/results/rekcat_enzymes.tsv"
#     uniprots = set()
#     pdbs = set()
    
#     if os.path.exists(dest_df_path):
#         df = pd.read_csv(dest_df_path, sep="\t")
        
#         # ugh, parse strings

#         for key, collection in (('pdb', pdbs), ('uniprot', uniprots)):
#             for x in df[key]:
#                 # remove []
#                 if pd.isna(x):
#                     continue
#                 if x.startswith("['") and x.endswith("']"):
#                     x = x[2:-2]
#                     # split by ', '
#                     parts = [y.upper() for y in x.split("', '")]
#                     collection.update(parts)
#                 else:
#                     collection.update(x.split(", "))
        
#     else:
#         df = search_folder_for_enzyme_idents("C:/conjunct/tmp/brenda_rekcat_pdfs")
#         # apply lambda x: ', '.join(x) to pdb, uniprot, refseq, genbank
#         for col in ['pdb', 'uniprot', 'refseq', 'genbank']:
#             df[col] = df[col].apply(lambda x: ', '.join(x))
        
#         df.to_csv(dest_df_path, sep="\t", index=False) # tsv because many commas in our sets
   
#     uniprot_path = "fetch_sequences/results/rekcat_uniprots.tsv"
#     if not os.path.exists(uniprot_path):
#         from kcatextract.fetch_sequences.query_idents import fetch_uniprots
        
#         uniprot_df = pd.DataFrame(columns=['uniprot', 'enzyme', 'organism'])

#         # do batches of 50
#         uniprots = list(tqdm(uniprots))
#         for i in range(0, len(uniprots), 50):
#             batch = uniprots[i:i+50]
#             try:
#                 appendage = fetch_uniprots(batch)
#             except Exception as e:
#                 print("Error fetching", batch)
#                 print(e)
#                 continue
#             uniprot_df = pd.concat([uniprot_df, appendage])
            
#             # wait for rate limit
#             import time
#             time.sleep(2)
            
#         uniprot_df.to_csv(uniprot_path, sep="\t", index=False)

#     # Print or further process the results
#     # for id, info in results["pdb"].items():
#         # turn sets into lists
#         # info['pmids'] = list(info['pmids'])
#         # print(f"ID: {id}")
#         # print(f"Enzyme: {info['enzyme']}")
#         # print(f"Organism: {info['organism']}")
#         # print(f"PMIDs: {', '.join(info['pmids'])}")
#         # print()
#     pdb_path = "fetch_sequences/results/rekcat_pdbs.tsv"
#     if not os.path.exists(pdb_path):
#         pdb_df = pd.DataFrame()# columns=['pdb', 'enzyme', 'organism'])
        
#         pdbs = list(pdbs)
#         for i in tqdm(range(0, len(pdbs), 50)):
#             batch = pdbs[i:i+50]
#             try:
#                 appendage = fetch_pdbs(batch)
#             except Exception as e:
#                 # print("Error fetching", batch)
#                 # print(e)
#                 # continue
#                 raise e
#             pdb_df = pd.concat([pdb_df, appendage])
            
#             # wait for rate limit
#             import time
#             time.sleep(2)
#     # write to file
#         pdb_df.to_csv(pdb_path, sep="\t", index=False)

# def script1():
#     # obtain mutant idents
#     dest_df_path = "fetch_sequences/results/rekcat_mutants.tsv"
#     # if not os.path.exists(dest_df_path):
#     df = search_folder_for_mutants("C:/conjunct/tmp/brenda_rekcat_pdfs")
#     df.to_csv(dest_df_path, sep="\t", index=False)
# if __name__ == "__main__":
#     script0()
    
    
         
    