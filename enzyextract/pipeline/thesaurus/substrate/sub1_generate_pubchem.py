"""
Useful to know:
https://jcheminf.biomedcentral.com/articles/10.1186/s13321-024-00868-3
https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/
https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/README-Extras
"""

import os
import polars as pl


# a solid ~4 GB of RAM needed
def create_synonym_parquet():
    df = pl.read_csv(
        "path/to/CID-Synonym-filtered.gz",
        has_header=False,
        separator="\t",
        quote_char=None,
        new_columns=["cid", "name"],
    )
    df.write_parquet("path/to/CID-Synonym-filtered.parquet")


def create_title_parquet():
    title_df = pl.scan_csv(
        "path/to/CID-Title.tsv",
        separator="\t",
        quote_char=None,
        has_header=False,
        new_columns=["cid", "title"],
    )
    title_df.sink_parquet("path/to/CID-Title.parquet")


def create_title_tsv():
    """
    it's just too many rows. maybe let's just stream into a tsv
    """
    exclude_df = pl.read_parquet("path/to/CID-Synonyms-unique-CIDs.parquet")
    exclude_set = set(exclude_df["cid"])
    del exclude_df

    from tqdm import tqdm

    ctr = 0
    with open("path/to/CID-Title.tsv", "w") as fout:
        with open("path/to/CID-Title", "r") as fin:
            for line in tqdm(fin, total=119108892):
                cid, title = line.split("\t", 1)
                if len(title) > 250:
                    continue
                    # note that the max length of a substrate is 185,
                    # so we can be safe and exclude anything longer than 250
                if int(cid) in exclude_set:
                    continue
                fout.write(f"{cid}\t{title}")
                ctr += 1
    # 60093403 lines,
    # 6.5 GB --> 5.587247  GB, 93 bytes per line 
    # (including bytes that represent the numerical index)
    print("Wrote", ctr, "lines")


# oops, need to add in a row
def main():
    cids = pl.read_parquet("path/to/CID-Synonym-filtered.parquet")
    # last CID is 172420250
    # expect 172 million rows

    # (103418344, 2) rows are here
    print(cids.shape)

    unique_cids = cids["cid"].unique()
    unique_cids.to_frame("cid").write_parquet(
        "path/to/CID-Synonyms-unique-CIDs.parquet"
    )
    print(len(unique_cids))


def preview_title_tsv():
    with open("path/to/CID-Title.tsv", "r") as f:
        for i, line in enumerate(f):
            if i > 10:
                break
            print(line)


if __name__ == "__main__":
    os.makedirs("path/to", exist_ok=True)

    # create_title_tsv()
    # create_synonym_parquet()
    # main()
    # preview_title_tsv()
    # create_title_parquet()

    df = pl.read_parquet("path/to/CID-Title.parquet")
    # yay we can load it into RAM!
    print(df["title"].str.len_chars().mean())  # 83.6 chars
