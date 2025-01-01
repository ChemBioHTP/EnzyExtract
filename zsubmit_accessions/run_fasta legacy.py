import os
from Bio import Entrez, SeqIO
import time

import pandas as pd
import requests
from tenacity import retry, wait_exponential
from tqdm import tqdm

from enzyextract.fetch_sequences.fasta_for_genbank import download_fasta_for_pdbs, download_fasta
from enzyextract.fetch_sequences.query_idents import fetch_pdbs, fetch_uniprots, fetch_ncbis

# def main_script_for_pdb(df):
#     # Example usage
#     # pdb_ids = ["1P5D"]  # Replace with your list of PDB IDs
#     from tqdm import tqdm
#     # pmid2seq = pd.read_csv('fetch_sequences/results/rekcat_enzymes.tsv', sep="\t")
#     pmid2seq = df
#     pdb_ids = set()
#     for i, row in pmid2seq.iterrows():
#         if pd.notnull(row['pdb']):
#             pdb_ids.update(row['pdb'].split(", "))
    
#     output_directory = "fetch_sequences/pdb"  # Replace with your desired output directory
#     download_fasta_for_pdbs(tqdm(pdb_ids), output_directory)
    
# def get_tsv_for_pdb(all_pdbs, namespace):
#     pdb_path = f"fetch_sequences/results/{namespace}_pdbs.tsv"
    
#     fails = set()
#     with open('fetch_sequences/results/pdb_fails.txt', 'r') as fails_file:
#         for line in fails_file:
#             fails.add(line.strip())
        
#         if not os.path.exists(pdb_path):
#             pdb_df = pd.DataFrame()# columns=['pdb', 'enzyme', 'organism'])
            
#             pdbs = list(all_pdbs)
#             for i in tqdm(range(0, len(pdbs), 50)):
#                 batch = pdbs[i:i+50]
#                 try:
#                     appendage = fetch_pdbs(batch)
#                 except Exception as e:
#                     print("Error fetching", batch)
#                     print(e)
#                     continue
#                     # raise e
#                 pdb_df = pd.concat([pdb_df, appendage])
                
#                 # wait for rate limit
#                 import time
#                 time.sleep(2)
#         # write to file
#             pdb_df.to_csv(pdb_path, sep="\t", index=False)

def new_get_tsv_for_pdb(df, namespace, last_downloaded=None, searched_pdf_filepath='fetch_sequences/pdbs_searched.txt'):
    pdb_path = f"fetch_sequences/results/pdb_fragments/{namespace}_pdbs.tsv"
    
    # searched pdbs
    with open(searched_pdf_filepath, 'r') as searched_file:
        searched_pdbs = set([line.strip() for line in searched_file])
    
    with open(searched_pdf_filepath, 'a+') as searched_file:
        # fails = set()
        # with open('fetch_sequences/results/pdb_fails.txt', 'r') as fails_file:
        #     for line in fails_file:
        #         fails.add(line.strip())
        
        if os.path.exists(pdb_path):
            pdb_df = pd.read_csv(pdb_path, sep="\t")
            if last_downloaded is None and not pdb_df.empty:
                last_downloaded = pdb_df['pdb'].iloc[-1]
        else:
            pdb_df = pd.DataFrame()
        
        # pdbs = sorted(set(all_pdbs))
        pdbs = set()
        for i, row in df.iterrows():
            if pd.notnull(row['pdb']):
                for pdb in row['pdb'].split(", "):
                    pdbs.add(pdb.upper())
                # pdbs.update(row['pdb'].split(", "))
        
        all_pdbs = sorted(list(pdbs)) # fix the order
        
        # now cull that which we have seen
        pdbs = [pdb for pdb in all_pdbs if pdb and pdb not in searched_pdbs]
        
        print(f"Searching {len(all_pdbs)} --> {len(pdbs)} PDBs")
        
        if last_downloaded:
            # if not last_downloaded in pdbs:
            #     raise ValueError(f"last_downloaded {last_downloaded} not in all_pdbs")
            start_index = pdbs.index(last_downloaded) + 1 if last_downloaded in pdbs else 0
        else:
            start_index = 0
        
        for i in tqdm(range(start_index, len(pdbs), 50)):
            batch = pdbs[i:i+50]
            try:
                appendage = fetch_pdbs(batch)
            except Exception as e:
                print("Error fetching", batch)
                print(e)
                continue
            
            for pdb in batch:
                # searched_pdbs.add(pdb)
                searched_file.write(pdb + "\n")
            
            pdb_df = pd.concat([pdb_df, appendage], ignore_index=True)
            
            # Continuously update the file
            pdb_df.to_csv(pdb_path, sep="\t", index=False)
            
            # wait for rate limit
            # flush
            searched_file.flush()
            time.sleep(2)

# def main_script_for_ncbi(df, last_downloaded=None):
#     # Set your email address (required by NCBI)
#     Entrez.email = "galen.wei@vanderbilt.com"

#     # accession_numbers = ["AAH44107"] # AF_125042"]
    
#     # df = pd.read_csv('fetch_sequences/results/rekcat_enzymes.tsv', sep="\t")
#     idents = set()
#     for i, row in df.iterrows():
#         for ident in ['genbank', 'refseq']:
#             if pd.notnull(row[ident]):
#                 val = row[ident]
#                 if val.startswith("['") and val.endswith("']"):
#                     val = val[2:-2]
#                     parts = val.split("', '")
#                 else:
#                     parts = val.split(", ")
#                 for x in parts:
#                     idents.add(x)
            

#     output_dir = "fetch_sequences/genbank"
#     os.makedirs(output_dir, exist_ok=True)
    
#     # Download FASTA files for each accession number
#     idents = sorted(list(idents))
    
#     # last_downloaded = 'CAC48392'
#     # last_downloaded = None
#     for accession in idents:
#         if last_downloaded and accession < last_downloaded:
#             continue
#         elif accession == last_downloaded:
#             print("Continuing from", accession)
#         download_fasta(accession, output_dir=output_dir)
#         time.sleep(1)  # Be nice to NCBI servers by adding a short delay between requests

#     print("Download complete.")

def main_script_for_uniprot(df, namespace, 
                            searched_pdf_filepath='fetch_sequences/uniprots_searched.txt',
                            chunk_size=50
                            ):
    
    write_dest = f"fetch_sequences/results/uniprot_fragments/{namespace}_uniprots.tsv"
    
    idents = set()
    for i, row in df.iterrows():
        if pd.notnull(row['uniprot']):
            for ident in row['uniprot'].split(", "):
                idents.add(ident.upper()) # IMPORTANT: deduplicate
    
    idents = sorted(list(idents))
    
    with open(searched_pdf_filepath, 'r') as f:
        seen = set([line.strip() for line in f])
    
    
    # also remove those already present in the file
    if os.path.exists(write_dest):
        uniprot_df = pd.read_csv(write_dest, sep="\t")
        seen.update(uniprot_df['uniprot'].values)
    else:
        uniprot_df = pd.DataFrame()
    
    idents = [ident for ident in idents if ident and ident not in seen]

    
    with open(searched_pdf_filepath, 'a+') as f:
        
        for i in tqdm(range(0, len(idents), chunk_size)):
            batch = idents[i:i+chunk_size]
            try:
                appendage = fetch_uniprots(batch)
            except Exception as e:
                print("Error fetching", batch)
                print(e)
                continue
            
            for pdb in batch:
                # searched_pdbs.add(pdb)
                f.write(pdb + "\n")
            
            uniprot_df = pd.concat([uniprot_df, appendage], ignore_index=True)
            
            # Continuously update the file
            uniprot_df.to_csv(write_dest, sep="\t", index=False)
            
            # wait for rate limit
            # flush
            f.flush()
            time.sleep(1)


def main_script_for_ncbi(df, namespace, 
                            searched_pdf_filepath='fetch_sequences/ncbis_searched.txt',
                            searched_auxiliaries=['fetch_sequences/uniprots_searched.txt'],
                            chunk_size=50
                            ):
    
    Entrez.email = "galen.wei@vanderbilt.edu"
    write_dest = f"fetch_sequences/results/ncbi_fragments/{namespace}_ncbis.tsv"
    
    idents = set()
    for i, row in df.iterrows():
        for db_type in ['genbank', 'refseq']:
            if pd.notnull(row[db_type]):
                for ident in row[db_type].split(", "):
                    idents.add(ident.upper()) # IMPORTANT: deduplicate
    
    idents = sorted(list(idents))
    
    with open(searched_pdf_filepath, 'r') as f:
        seen = set([line.strip() for line in f])
    for filepath in searched_auxiliaries:
        with open(filepath, 'r') as f:
            seen.update([line.strip() for line in f])
    
    
    # also remove those already present in the file
    if os.path.exists(write_dest):
        ncbi_df = pd.read_csv(write_dest, sep="\t")
        seen.update(ncbi_df['ncbi'].values)
    else:
        ncbi_df = pd.DataFrame()
    
    len_before = len(idents)
    idents = [ident for ident in idents if ident and ident not in seen]

    print(f"Reduced {len_before} --> {len(idents)}")
    
    with open(searched_pdf_filepath, 'a+') as f:
        
        for i in tqdm(range(0, len(idents), chunk_size)):
            batch = idents[i:i+chunk_size]
            try:
                appendage = fetch_ncbis(batch)
            except Exception as e:
                # print("Error fetching", batch)
                print(e)
                time.sleep(1)
                continue
            finally:
                time.sleep(1)
            
            for pdb in batch:
                # searched_pdbs.add(pdb)
                f.write(pdb + "\n")
            
            ncbi_df = pd.concat([ncbi_df, appendage], ignore_index=True)
            
            # Continuously update the file
            ncbi_df.to_csv(write_dest, sep="\t", index=False)
            
            # wait for rate limit
            # flush
            f.flush()
            # time.sleep(1)

def script2(df):
    # fix boo boo:
    
    pdbs = set()
    for i, row in df.iterrows():
        if pd.notnull(row['pdb']):
            pdbs.update(row['pdb'].split(", "))
    
    all_pdbs = sorted(list(pdbs)) # fix the order
    
    # get last downloaded
    pdb_path = f"fetch_sequences/results/scratch_open_pdbs.tsv"
    if os.path.exists(pdb_path):
        pdb_df = pd.read_csv(pdb_path, sep="\t")
        last_downloaded = pdb_df['pdb'].iloc[-1]
    else:
        last_downloaded = None
    
    print(f"Last downloaded: {last_downloaded}")
    
    # chop off the version
    if '_' in last_downloaded:
        last_downloaded = last_downloaded.split('_', 1)[0]
    # take all pdbs from start to last downloaded and ', '.join
    
    print(f"Last downloaded: at index {all_pdbs.index(last_downloaded)} out of {len(all_pdbs)}")
    
    # now write all these to fetch_sequences/pdb_fragments/pdbs_searched.txt
    with open('fetch_sequences/pdbs_searched.txt', 'w') as searched_file:
        for pdb in all_pdbs[:all_pdbs.index(last_downloaded)+1]: # include last downloaded
            searched_file.write(pdb + "\n")
    
    exit(0)

def script3():
    # compile tried pdbs from rekcat
    df = pd.read_csv('fetch_sequences/results/pdb_fragments/rekcat_pdbs.tsv', sep="\t")
    pdbs = set()
    for i, row in df.iterrows():
        pdb = row['pdb']
        # unversion
        if '_' in pdb:
            pdb = pdb.split('_', 1)[0]
        pdbs.add(pdb)
    
    pdbs = sorted(set(pdbs))
    # write to searched file
    with open('fetch_sequences/results/pdb_fragments/pdbs_searched_2.tsv', 'w') as searched_file:
        for pdb in pdbs:
            searched_file.write(pdb + "\n")
    
    exit(0)

def reassess_uniprot():
    # compile tried pdbs from fragments
    # from an outdated log file
    with open('fetch_sequences/uniprots_searched.txt', 'r') as searched_file:
        searched_uniprots = set([line.strip() for line in searched_file])
    
    count = 0
    with open('fetch_sequences/uniprots_searched.txt', 'a+') as searched_file:
        for filename in os.listdir('fetch_sequences/results/uniprot_fragments/'):
            if filename.endswith('.tsv'):
                df = pd.read_csv(f'fetch_sequences/results/uniprot_fragments/{filename}', sep="\t")
            elif filename.endswith('.csv'):
                df = pd.read_csv(f'fetch_sequences/results/uniprot_fragments/{filename}')
            uniprots = sorted(set(df['uniprot']))
            for uniprot in uniprots:
                if uniprot not in searched_uniprots:
                    searched_file.write(uniprot + "\n")
                    searched_uniprots.add(uniprot)
                    count += 1
    print(f"Added {count} uniprots")
    exit(0)
                


if __name__ == "__main__":
    # df = pd.read_csv('fetch_sequences/results/rekcat_enzymes.tsv', sep="\t")
    # script3()

    # reassess_uniprot()
    # df = pd.read_csv('fetch_sequences/readpdf/rekcat_enzymes.tsv', sep="\t")
    df = pd.read_csv('fetch_sequences/readpdf/all_nonbrenda_dbids.tsv', sep="\t")


    # get_tsv_for_pdb()
    main_script_for_ncbi(df, 'nonbrenda') # last_downloaded = 'GM078360')
    # new_get_tsv_for_pdb(df, 'nonbrenda')
    # main_script_for_uniprot(df, 'nonbrenda')
    