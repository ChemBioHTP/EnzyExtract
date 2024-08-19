import os
from Bio import Entrez, SeqIO
import time

import pandas as pd
from tenacity import retry, wait_exponential
from tqdm import tqdm

def fetch_fasta_by_id(identifier, db="protein"):
    """
    Fetch FASTA sequence by identifier from the specified database.
    
    Args:
    - identifier (str): The identifier (RefSeq or GenBank).
    - db (str): The database to search. Default is "protein".
    
    Returns:
    - str: The FASTA sequence as a string.
    """
    try:
        with Entrez.efetch(db=db, id=identifier, rettype="fasta", retmode="text") as handle:
            return handle.read(), None
        # only catch 400
    except Exception as e:
        if "HTTP Error 400: Bad Request" in str(e):
            return None, f"{identifier} {db} not found"
        else:
            raise e

# exponential backoff
@retry(wait=wait_exponential(multiplier=1, max=60))
def download_fasta(accession, output_dir="fetch_sequences/genbank"):
    output_file = f"{output_dir}/{accession}.fasta"
    if os.path.exists(output_file):
        print(f"Already downloaded: {accession}")
        return
    fasta, err = fetch_fasta_by_id(accession)
    # protein = True
    if not fasta:
        # try nucleotide
        time.sleep(1)
        fasta, err2 = fetch_fasta_by_id(accession, db="nucleotide")
        # protein = False
        # it's obvious which fasta are nucleotide
    if fasta:
        print(f"Downloaded: {accession}")
        with open(output_file, "w") as f:
            f.write(fasta)
    else:
        print(f"Error fetching {accession}: {err} and {err2}")



def download_fasta_for_pdbs(pdb_ids, output_dir=".", wait=0.3):
    """
    Downloads FASTA sequences for a list of PDB IDs.

    :param pdb_ids: List of PDB IDs to download FASTA sequences for.
    :param output_dir: Directory where the FASTA files will be saved.
    """
    base_url = "https://www.rcsb.org/fasta/entry/"
    
    for pdb_id in pdb_ids:
        url = f"{base_url}{pdb_id}"
        response = requests.get(url)
        
        if response.status_code == 200:
            fasta_content = response.text
            output_file = f"{output_dir}/{pdb_id}.fasta"
            with open(output_file, "w") as f:
                f.write(fasta_content)
            # print(f"Downloaded {pdb_id}.fasta")
        elif response.status_code == 404:
            print(f"PDB ID not found: {pdb_id}")
        else:
            raise Exception(f"Error fetching {pdb_id}: {response.status_code}")
        
        time.sleep(wait)

def main_script_for_pdb():
    # Example usage
    # pdb_ids = ["1P5D"]  # Replace with your list of PDB IDs
    from tqdm import tqdm
    pmid2seq = pd.read_csv('fetch_sequences/results/rekcat_enzymes.tsv', sep="\t")
    pdb_ids = set()
    for i, row in pmid2seq.iterrows():
        if pd.notnull(row['pdb']):
            pdb_ids.update(row['pdb'].split(", "))
    
    output_directory = "fetch_sequences/pdb"  # Replace with your desired output directory
    download_fasta_for_pdbs(tqdm(pdb_ids), output_directory)
    
if __name__ == "__main__":
    # Set your email address (required by NCBI)
    Entrez.email = "galen.wei@vanderbilt.com"

    # accession_numbers = ["AAH44107"] # AF_125042"]
    
    df = pd.read_csv('fetch_sequences/results/rekcat_enzymes.tsv', sep="\t")
    idents = set()
    for i, row in df.iterrows():
        for ident in ['genbank', 'refseq']:
            if pd.notnull(row[ident]):
                val = row[ident]
                if val.startswith("['") and val.endswith("']"):
                    val = val[2:-2]
                    for x in val.split("', '"):
                        idents.add(x)
            

    output_dir = "fetch_sequences/genbank"
    os.makedirs(output_dir, exist_ok=True)
    
    # Download FASTA files for each accession number
    idents = sorted(list(idents))
    
    last_downloaded = 'CAC48392'
    for accession in idents:
        if last_downloaded and accession < last_downloaded:
            continue
        elif accession == last_downloaded:
            print("Continuing from", accession)
        download_fasta(accession, output_dir=output_dir)
        time.sleep(1)  # Be nice to NCBI servers by adding a short delay between requests

    print("Download complete.")