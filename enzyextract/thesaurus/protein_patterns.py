import re

pdb_pattern = re.compile(r'\b[1-9][A-Z0-9]{3}\b')
# not all numbers
pdb_pattern_stricter = re.compile(r'\b(\d[A-Z][A-Z0-9][A-Z0-9]|\d[A-Z0-9][A-Z][A-Z0-9]|\d[A-Z0-9][A-Z0-9][A-Z](?:_\d+)?)\b')
pdb_pattern_stricter_i = re.compile(r'(?i)\b(\d[A-Z][A-Z0-9][A-Z0-9]|\d[A-Z0-9][A-Z][A-Z0-9]|\d[A-Z0-9][A-Z0-9][A-Z](?:_\d+)?)\b')

protein_data_bank_pattern = re.compile(r'(?i)\b(PDB|protein\s*data\s*bank)\b')
"""
This looks strictly for the words "PDB" or "protein data bank" to accompany PDB ids
"""

# uniprot_pattern = re.compile(r'\b[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9]\b', re.IGNORECASE)
# uniprot_v2_pattern = re.compile(r'\b[OPQ][0-9][A-Z0-9]{3}[0-9]\b', re.IGNORECASE)
# uniprot_v3_pattern = re.compile(r'\b[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9][A-Z0-9]{2}[0-9]\b', re.IGNORECASE)
# OOF ^ the above is wrong!
uniprot_pattern = re.compile(r'[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2}')
uniprot_blacklist = ['K2HPO4', "C8H9O2", 'H2S2O3'] # K2HP04
# ncbi_pattern = re.compile(r'\b[ANWCTSG][0-9]+\b')

refseq_pattern = re.compile(r'\b((?:A[CP]|N[CGTWZMRP]|X[MRP]|YP|WP)_\d+(?:\.\d+)?)\b')

genbank_pattern = re.compile(r'\b([A-Z]{1,3}\d{5,8}(?:\.\d+)?)\b')