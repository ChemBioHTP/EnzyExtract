
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

# amino1 = "ACDEFGHIKLMNPQRSTVWY"
# amino3 = "Ala|Cys|Asp|Glu|Phe|Gly|His|Ile|Lys|Leu|Met|Asn|Pro|Gln|Arg|Ser|Thr|Val|Trp|Tyr"
# mutant_pattern = re.compile(r'\b([ACDEFGHIKLMNPQRSTVWY][1-9]\d{1,3}[ACDEFGHIKLMNPQRSTVWY])\b')
# mutant_v2_pattern = re.compile(rf'\b(?:{amino3})[1-9]\d{{1,3}}(?:{amino3})\b', re.IGNORECASE)
# mutant_v3_pattern = re.compile(rf'\b((?:{amino3})[1-9]\d{{0,3}}(?:{amino3}))\b', re.IGNORECASE)

# mutant_v4_pattern = re.compile(rf'\b((?:{amino3})-?[1-9]\d{{1,3}})\b', re.IGNORECASE)

from enzyextract.thesaurus.mutant_patterns import amino1, amino3, mutant_pattern, mutant_v3_pattern
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

    
         
    