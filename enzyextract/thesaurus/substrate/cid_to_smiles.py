# Written Dec 25 2024

import polars as pl

import gzip
import csv
from tqdm import tqdm


def produce_cid_to_smiles(
    all_cids: set[int],
    cid_smiles_gz: str = 'data/substrates/cid/CID-SMILES.gz',
    write_dest: str = 'data/substrates/cid/cid2smiles.tsv'
):
    """
    load up CID-SMILES.gz, which has 119314432 rows, and can be found at:
    ftp://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/CID-SMILES.gz
    produce subset of CID that is of interest.

    all_cids is set of pubchem ids.
    """
    print("Regenerating cid2smiles (will cache)")


    # Stream read the file and filter rows
    with open(write_dest, 'w') as sink:
        sink.write("CID\tSMILES\n")
        with gzip.open(cid_smiles_gz, 'rt', encoding='utf-8', errors='replace') as gzfile:
            reader = csv.reader(gzfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in tqdm(reader):
                # 1st element is the CID, 2nd element is the title
                try:
                    cid = row[0]
                    smiles = row[1]
                    if int(cid) in all_cids:
                        sink.write(f"{cid}\t{smiles}\n")
                except IndexError:
                    print("Malformed row:", row)
                
if __name__ == '__main__':

    df1 = pl.read_csv('data/substrates/cid/cid2title.tsv', separator='\t')
    df2 = pl.read_csv('data/substrates/cid/cid2synonym.tsv', separator='\t')

    df = pl.concat([df1, df2])
    all_cids = set(df['CID'].unique().drop_nulls())

    # print(len(want_cids)) # 57571
    produce_cid_to_smiles(all_cids)