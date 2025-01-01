import os
from typing import Callable
from Bio import Entrez, SeqIO
import time

# import pandas as pd
import polars as pl
import polars.selectors as cs
import requests
from tenacity import retry, wait_exponential
from tqdm import tqdm

try:
    from enzyextract.fetch_sequences.query_idents import fetch_pdbs, fetch_uniprots, fetch_ncbis
    from enzyextract.fetch_sequences.query_uniprot import fetch_uniprots_expanded, fetch_uniparc, fetch_uniprots_individually, fetch_uniprots_latest
except ImportError:
    from query_idents import fetch_pdbs, fetch_uniprots, fetch_ncbis
    from query_uniprot import fetch_uniprots_expanded, fetch_uniparc, fetch_uniprots_individually, fetch_uniprots_latest

def read_all_dfs(folderpath, blacklist: Callable[[str], bool] = None, so=None, allow_empty=True) -> pl.DataFrame:
    """
    Read all dataframes in a folder and concatenate them.
    """
    dfs = []
    for filename in os.listdir(folderpath):
        if blacklist is not None and blacklist(filename):
            continue
        if filename.endswith('.tsv'):
            df = pl.read_csv(f'{folderpath}/{filename}', separator='\t', schema_overrides=so)
            dfs.append(df)
        elif filename.endswith('.csv'):
            df = pl.read_csv(f'{folderpath}/{filename}', schema_overrides=so)
            dfs.append(df)
        elif filename.endswith('.parquet'):
            df = pl.read_parquet(f'{folderpath}/{filename}')
            dfs.append(df)
    if allow_empty and len(dfs) == 0:
        return pl.DataFrame()
    return pl.concat(dfs, how='diagonal')


def submit_script_pdb(df: pl.DataFrame, write_to):
    """
    df: a polars dataframe with a column 'pdb' (pl.Utf8) that contains the pdb id.
    write_to: the path to write the parquet file to.
    """
    
    assert not os.path.exists(write_to), "PDB file already exists: " + write_to

    pdbs = df['pdb'].drop_nulls().unique().sort().to_list()
    print(f"Searching {len(pdbs)} PDBs")

    pdb_df = pl.DataFrame()
    for i in tqdm(range(0, len(pdbs), 50)):
        batch = pdbs[i:i+50]
        try:
            appendage = fetch_pdbs(batch)
            appendage = pl.from_pandas(appendage)
        except Exception as e:
            print("Error fetching", batch)
            print(e)
            if pdb_df.height == 0:
                raise e # if no data has been fetched, raise the error
            fail_df = pl.DataFrame({'pdb': batch})
            pdb_df = pl.concat([pdb_df, fail_df], how='diagonal')
            continue
            
        
        pdb_df = pl.concat([pdb_df, appendage], how='diagonal')
        
        # Continuously update the file
        pdb_df.write_parquet(write_to)
        
        # wait for rate limit
        time.sleep(2)
    
# UPI0002CCC44A

def submit_script_uniprot(df: pl.DataFrame, write_to,
                            chunk_size=50,
                            expanded=True,
                            individually=False,
                            uniparc=False,
                            legacy=False,
                            ):
    """
    df: a polars dataframe with a column 'uniprot' (pl.Utf8) that contains the uniprot id.

    write_to: the path to write the parquet file to.
    
    """

    # Uniprot didn't return all 50?
    assert not os.path.exists(write_to), "Uniprot file already exists: " + write_to

    idents = df['uniprot'].drop_nulls().unique().sort().to_list()
    
    if individually:
        print("Fetching individually")
        uniprot_df = fetch_uniprots_individually(idents)
        uniprot_df.write_parquet(write_to)

    else:
        uniprot_df = pl.DataFrame()
        for i in tqdm(range(0, len(idents), chunk_size)):
            batch = idents[i:i+chunk_size]
            try:
                if uniparc:
                    appendage = fetch_uniparc(batch)
                elif legacy:
                    if expanded:
                        appendage = fetch_uniprots_expanded(batch)
                    else:
                        appendage = fetch_uniprots(batch)
                        appendage = pl.from_pandas(appendage)
                else:
                    appendage = fetch_uniprots_latest(batch)
            except Exception as e:
                print("Error fetching", batch)
                print(e)
                if uniprot_df.height == 0:
                    raise e # if no data has been fetched, raise the error
                fail_df = pl.DataFrame({'uniprot': batch})
                uniprot_df = pl.concat([uniprot_df, fail_df], how='diagonal')
                continue

            uniprot_df = pl.concat([uniprot_df, appendage], how='diagonal')
            
            # Continuously update the file
            uniprot_df.write_parquet(write_to)
            
            # wait for rate limit
            time.sleep(1)
        


def submit_script_ncbi(df: pl.DataFrame, write_to, db='protein',
                            chunk_size=50
                            ):
    """
    df: a polars DataFrame with a column 'ncbi' (pl.Utf8) that contains the ncbi id (genbank or refseq).

    write_to: the path to write the parquet file to.
    """
    assert not os.path.exists(write_to), "NCBI file already exists: " + write_to
    
    Entrez.email = "galen.wei@vanderbilt.edu"
    
    idents = df['ncbi'].drop_nulls().unique().sort().to_list()
    
    # also remove those already present in the file
    ncbi_df = pl.DataFrame()

    print(f"Processing {len(idents)} NCBI ids")
    
    for i in tqdm(range(0, len(idents), chunk_size)):
        batch = idents[i:i+chunk_size]
        try:
            appendage = fetch_ncbis(batch, db=db)
            appendage = pl.from_pandas(appendage)
        except Exception as e:
            print("Error fetching", batch)
            print(e)
            # if ncbi_df.height == 0:
                # raise e # if no data has been fetched, raise the error
            fail_df = pl.DataFrame({'ncbi': batch})
            ncbi_df = pl.concat([ncbi_df, fail_df], how='diagonal')
            continue

        ncbi_df = pl.concat([ncbi_df, appendage], how='diagonal')
        
        # Continuously update the file
        ncbi_df.write_parquet(write_to)
        
        # wait for rate limit
        time.sleep(1)
        


if __name__ == "__main__":

    # Format the current time
    ts = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    print("Current time:", ts)
    # UniProt
    # refseq failed
    working = 'uniprot'
    # working = 'uniparc'
    # working = 'uniprot_slow'
    processed = None
    if working == 'uniprot' or working == 'uniprot_slow':
        df = pl.read_parquet('data/enzymes/accessions/unknown/unknown_uniprot.parquet')
        fragment_folder = 'uniprot'
        col_name = 'uniprot'

        # processed = read_all_dfs(f'data/enzymes/accessions/uniprot')
        bdr = []
        for filename in os.listdir('data/enzymes/accessions/uniprot'):
            if filename.endswith('.parquet'):
                bdr.append(
                    pl.scan_parquet(f'data/enzymes/accessions/uniprot/{filename}').select(
                        cs.exclude('full_response')
                    ).collect()
                )
        processed = pl.concat(bdr, how='diagonal')
        # add in merged/demerged uniprots
        additional = processed.filter(
            pl.col('why_deleted').is_in(['merged', 'demerged'])
        ).select('uniprot_aliases').explode('uniprot_aliases').rename({'uniprot_aliases': 'uniprot'})
        # remove those which act as a secondary accession to a primary accession with a sequence
        secondaries = processed.filter(
            pl.col('sequence').is_not_null()
            & pl.col('uniprot_aliases').is_not_null()
        ).select('uniprot_aliases').explode('uniprot_aliases').rename({'uniprot_aliases': 'uniprot'})
        df = pl.concat([df, additional], how='diagonal')
        df = df.filter(~pl.col('uniprot').is_in(secondaries['uniprot']))
        pass


    elif working == 'uniparc':
        df = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
        df = df.filter(
            pl.col('why_deleted').is_not_null()
            & pl.col('uniparc').is_not_null()
        ).select('uniparc').unique().rename({'uniparc': 'uniprot'})
        fragment_folder = None
        col_name = 'uniprot'

    elif working == 'pdb':
        df = pl.read_parquet('data/enzymes/accessions/unknown/unknown_pdb.parquet')
        processed = read_all_dfs(f'data/enzymes/accessions/pdb')
        col_name = 'pdb'
    elif working == 'refseq':
        df = pl.read_parquet('data/enzymes/accessions/unknown/unknown_refseq.parquet')
        df = df.filter(
            pl.col('refseq').str.starts_with('NP_')
            | pl.col('refseq').str.starts_with('YP_')
            | pl.col('refseq').str.starts_with('XP_')
            | pl.col('refseq').str.starts_with('WP_')
        ) # only proteins
        df = df.rename({'refseq': 'ncbi'})
        processed = read_all_dfs(f'data/enzymes/accessions/refseq')
        col_name = 'ncbi'
    elif working == 'genbank':
        df = pl.read_parquet('data/enzymes/accessions/unknown/unknown_genbank.parquet')
        df = df.rename({'genbank': 'ncbi'})
        processed = read_all_dfs(f'data/enzymes/accessions/ncbi')
        col_name = 'ncbi'
    else:
        raise ValueError("Unknown working type")

    
    write_to = f'data/enzymes/accessions/{working}/{working}_{ts}.parquet'
    
    print("Read", df.height, working, "entries")

    if processed is not None and processed.height:
        df = df.filter(~pl.col(col_name).is_in(processed[col_name]))
    print("Keeping", df.height, "entries")
    print("Writing to", write_to)

    if working == 'uniprot':
        # perform some more pruning

        submit_script_uniprot(df, write_to)
    elif working == 'uniprot_slow':
        submit_script_uniprot(df, write_to, individually=True)
    elif working == 'uniparc':
        submit_script_uniprot(df, write_to, uniparc=True)
    elif working == 'pdb':
        submit_script_pdb(df, write_to)
    
    elif working == 'refseq':
        submit_script_ncbi(df, write_to, db='protein')
    
    elif working == 'genbank':
        submit_script_ncbi(df, write_to)
    else:
        raise ValueError("Unknown working type")


    