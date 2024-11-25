
import os
import re

from kcatextract.fetch_sequences.query_idents import fetch_pdbs
from kcatextract.fetch_sequences.read_pdfs_for_idents import search_folder_for_enzyme_idents, search_folder_for_mutants
import pandas as pd
from tqdm import tqdm

def read_pdfs_for_db_idents(namespace, src_folder, dest_df_path=None):
    # read folder of pdfs, get identifiers
    

    
    if dest_df_path is None:
        dest_df_path = f"fetch_sequences/readpdf/{namespace}_enzymes.tsv"
    uniprots = set()
    pdbs = set()
    
    
    collect_names = False # collecting FASTA files supercedes the need for  this
    if False: # os.path.exists(dest_df_path):
        df = pd.read_csv(dest_df_path, sep="\t")
        
        # ugh, parse strings

        for key, collection in (('pdb', pdbs), ('uniprot', uniprots)):
            for x in df[key]:
                # remove []
                if pd.isna(x):
                    continue
                if x.startswith("['") and x.endswith("']"):
                    x = x[2:-2]
                    # split by ', '
                    parts = [y.upper() for y in x.split("', '")]
                    collection.update(parts)
                else:
                    collection.update(x.split(", "))
        
    else:
        df = search_folder_for_enzyme_idents(src_folder)
        # apply lambda x: ', '.join(x) to pdb, uniprot, refseq, genbank
        for col in ['pdb', 'uniprot', 'refseq', 'genbank']:
            df[col] = df[col].apply(lambda x: ', '.join(x))
        
        # df.to_csv(dest_df_path, sep="\t", index=False) # tsv because many commas in our sets
        # append to path if possible
        if os.path.exists(dest_df_path):
            df.to_csv(dest_df_path,
                        sep="\t", index=False, mode='a', header=False)
        else:
            df.to_csv(dest_df_path,
                        sep="\t", index=False)
   
    if not collect_names:
        return
   
    uniprot_path = "fetch_sequences/results/rekcat_uniprots.tsv"
    if not os.path.exists(uniprot_path):
        from kcatextract.fetch_sequences.query_idents import fetch_uniprots
        
        uniprot_df = pd.DataFrame(columns=['uniprot', 'enzyme', 'organism'])

        # do batches of 50
        uniprots = list(tqdm(uniprots))
        for i in range(0, len(uniprots), 50):
            batch = uniprots[i:i+50]
            try:
                appendage = fetch_uniprots(batch)
            except Exception as e:
                print("Error fetching", batch)
                print(e)
                continue
            uniprot_df = pd.concat([uniprot_df, appendage])
            
            # wait for rate limit
            import time
            time.sleep(2)
            
        uniprot_df.to_csv(uniprot_path, sep="\t", index=False)

    # Print or further process the results
    # for id, info in results["pdb"].items():
        # turn sets into lists
        # info['pmids'] = list(info['pmids'])
        # print(f"ID: {id}")
        # print(f"Enzyme: {info['enzyme']}")
        # print(f"Organism: {info['organism']}")
        # print(f"PMIDs: {', '.join(info['pmids'])}")
        # print()
    pdb_path = "fetch_sequences/results/rekcat_pdbs.tsv"
    if not os.path.exists(pdb_path):
        pdb_df = pd.DataFrame(columns=['pdb', 'enzyme', 'organism'])
        
        pdbs = list(pdbs)
        for i in tqdm(range(0, len(pdbs), 50)):
            batch = pdbs[i:i+50]
            try:
                appendage = fetch_pdbs(batch)
            except Exception as e:
                # print("Error fetching", batch)
                # print(e)
                # continue
                raise e
            pdb_df = pd.concat([pdb_df, appendage])
            
            # wait for rate limit
            import time
            time.sleep(2)
    # write to file
        pdb_df.to_csv(pdb_path, sep="\t", index=False)

def read_pdfs_for_mutants(src_folder, dest_df_path):
    # obtain mutant idents
    # dest_path = "fetch_sequences/readpdf/scratch_wiley_enzymes.tsv"

    # if not os.path.exists(dest_df_path):
    # df = search_folder_for_mutants("C:/conjunct/tmp/brenda_rekcat_pdfs")
    # df.to_csv(dest_df_path, sep="\t", index=False)
    
    df = search_folder_for_mutants(src_folder)
    
    # incremental
    if os.path.exists(dest_df_path):
        df.to_csv(dest_df_path,
                    sep="\t", index=False, mode='a', header=False)
    else:
        df.to_csv(dest_df_path,
                    sep="\t", index=False)
    
if __name__ == "__main__":
    # namespace = "scratch_wiley"
    # # src_folder = "C:/conjunct/tmp/brenda_rekcat_pdfs"
    # src_folder = "D:/scratch/wiley"
    # dest_df_path = "fetch_sequences/readpdf/all_nonbrenda_dbids.tsv"
    dest_df_path = "fetch_sequences/readpdf/nonbrenda_mutants.tsv"

    sources = ["scratch/asm", "scratch/hindawi", "scratch/open", "scratch/wiley", "topoff/hindawi", "topoff/open", "topoff/wiley", "wos/asm", "wos/hindawi", "wos/jbc", "wos/local_shim", "wos/open", "wos/wiley"]
    for src_folder in sources:
        namespace = src_folder.replace("/", "_")
        # read_pdfs_for_db_idents(namespace, f"D:/{src_folder}", dest_df_path)
        read_pdfs_for_mutants(f"D:/{src_folder}", dest_df_path)
    # read_pdfs_for_db_idents()
    
    
         
    