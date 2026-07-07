import os
from pathlib import Path
from typing import Literal, Union
from Bio import Entrez
import time

# import pandas as pd
import polars as pl
from tqdm import tqdm

from enzyextract.dependency.injection import REQUIRE, resolve

from enzyextract.fetch_sequences.query_idents import fetch_pdbs, fetch_uniprots, fetch_ncbis
from enzyextract.fetch_sequences.query_uniprot import fetch_uniprots_expanded, fetch_uniparc, fetch_uniprots_individually, fetch_uniprots_latest
from enzyextract.thesaurus.enzyme_io import read_all_dfs


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
        


def submit_script_ncbi(
    df: pl.DataFrame, 
    write_to,
    db='protein',
    chunk_size=50,
    *,
    entrez_email=None,
):
    """
    df: a polars DataFrame with a column 'ncbi' (pl.Utf8) that contains the ncbi id (genbank or refseq).

    write_to: the path to write the parquet file to.
    """
    assert not os.path.exists(write_to), "NCBI file already exists: " + write_to
    
    if entrez_email is None:
        raise ValueError("Entrez_email must be set to a valid email address.")
    Entrez.email = entrez_email

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

def _makedirs_exist_ok(fpath):
    parent_dir = os.path.dirname(fpath)
    os.makedirs(parent_dir, exist_ok=True)


def accession_batch_downloader(
    working: Literal["uniprot", "uniparc", "uniprot_slow", "pdb", "refseq", "genbank"],
    entrez_email,
    df: pl.DataFrame,
    write_folder: Union[str, Path],
    processed: pl.DataFrame = None,
):
    """
    Main entry point for the script.

    df: a polars DataFrame that contains the accessions to process. Depending on the type of accession,
    it should have a column named `uniprot`, `pdb`, `refseq`, or `genbank`.

    processed: a polars DataFrame that contains the accessions that have already been processed. It should have a column
    named `uniprot`, `pdb`, `refseq`, or `genbank` depending on the working type.
    If not provided, then the script searches `{write_folder}/{working}` for existing parquet files.

    write_dest: the path to write the parquet file to. If not provided, it will be written to
    `{write_folder}/{working}/{working}_{timestamp}.parquet`.
    """

    # Format the current time
    ts = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    print("Current time:", ts)
    # UniProt
    # refseq failed

    if working in ["uniprot", "uniprot_slow", "uniparc"]:
        col_name = "uniprot"
    elif working in ["refseq", "genbank"]:
        col_name = "ncbi"
    elif working in ["pdb"]:
        col_name = "pdb"
    else:
        raise ValueError("Unknown working type")

    # Small modifications to df
    if working == "uniparc":
        df = df.rename({"uniparc": "uniprot"}, strict=False)
    elif working == "refseq":
        df = df.filter(
            pl.col("refseq").str.starts_with("NP_")
            | pl.col("refseq").str.starts_with("YP_")
            | pl.col("refseq").str.starts_with("XP_")
            | pl.col("refseq").str.starts_with("WP_")
        ) # only proteins
        df = df.rename({"refseq": "ncbi"}, strict=False)
    elif working == "genbank":
        df = df.rename({"genbank": "ncbi"}, strict=False)


    if processed is None:
        if working == "uniprot" or working == "uniprot_slow":
            processed = read_all_dfs(f"{write_folder}/{working}")

        elif working == "pdb":
            processed = read_all_dfs(f"{write_folder}/{working}")
        elif working == "refseq":
            processed = read_all_dfs(f"{write_folder}/{working}")
        elif working == "genbank":
            processed = read_all_dfs(f"{write_folder}/{working}")

    
    write_to = f"{write_folder}/{working}/{working}_{ts}.parquet"
    _makedirs_exist_ok(write_to)

    print("Read", df.height, working, "entries")

    if processed is not None and processed.height:
        df = df.filter(~pl.col(col_name).is_in(processed[col_name]))

    # adjust pdbs based on first 4 characters (ignoring version differences)
    if working == "pdb" and processed is not None:
        df = df.with_columns(
            pl.col("pdb").str.to_lowercase().str.slice(0, 4).alias("pdb_temp")
        ).join(
            processed.select(
                pl.col("pdb").str.to_lowercase().str.slice(0, 4).alias("pdb_temp")
            ),
            on="pdb_temp",
            how="anti",
        ).drop("pdb_temp")
    elif working in ["refseq", "genbank"] and processed is not None:
        # remove .\d version
        df = df.with_columns(
            pl.col("ncbi").str.replace(r"\.\d+$", "").alias("ncbi_temp")
        ).join(
            processed.with_columns(
                pl.col("ncbi").str.replace(r"\.\d+$", "").alias("ncbi_temp")
            ).select("ncbi_temp"),
            on="ncbi_temp",
            how="anti",
        ).drop("ncbi_temp")

    print("Keeping", df.height, "entries")
    print("Writing to", write_to)

    if working == "uniprot":
        # perform some more pruning

        submit_script_uniprot(df, write_to)
    elif working == "uniprot_slow":
        submit_script_uniprot(df, write_to, individually=True)
    elif working == "uniparc":
        submit_script_uniprot(df, write_to, uniparc=True)
    elif working == "pdb":
        submit_script_pdb(df, write_to)
    
    elif working == "refseq":
        submit_script_ncbi(df, write_to, db="protein", entrez_email=entrez_email)
    
    elif working == "genbank":
        submit_script_ncbi(df, write_to, entrez_email=entrez_email)
    else:
        raise ValueError("Unknown working type")

@resolve
def _main(
    df = REQUIRE('data/enzymes/accessions/unknown/unknown_uniprot.parquet')
):
    accession_batch_downloader(
        # entrez_email=Your email here
        working='uniprot',
        df=df
    )


if __name__ == "__main__":
    raise RuntimeError("This script is only an example.")
    _main()
    pass