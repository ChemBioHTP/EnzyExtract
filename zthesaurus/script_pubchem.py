import polars as pl
# useful to know:
# https://jcheminf.biomedcentral.com/articles/10.1186/s13321-024-00868-3
# https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/
# https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/Extras/README-Extras

# preview first 10 lines of C:\conjunct\bigdata\pubchem\CID-Synonym-filtered.tsv

# a solid ~4 GB of RAM needed
def create_synonym_parquet():
    pass
    df = pl.read_csv('C:/conjunct/bigdata/pubchem/CID-Synonym-filtered.gz', separator='\t', quote_char=None)
    # print(df.head(10))
    df = df.rename({"1": "cid", "Acetyl-DL-carnitine": "name"})
    header_as_row = pl.DataFrame(
        [[1, "Acetyl-DL-carnitine"]], 
        schema=['cid', 'name'],
        orient='row',
    )  # Create a single-row DataFrame for the header
    df = header_as_row.vstack(df)  # Prepend the header row to the original DataFrame
    return df

def create_title_parquet():
    title_df = pl.scan_csv('C:/conjunct/bigdata/pubchem/CID-Title.tsv', 
                     separator='\t', quote_char=None, has_header=False)
    title_df = title_df.rename({"column_1": "cid", "column_2": "title"})
    title_df.sink_parquet('C:/conjunct/bigdata/pubchem/CID-Title.parquet')
    # exclude_df = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Synonyms-unique-CIDs.parquet')
    # title_df.filter(
    #     ~pl.col('cid').is_in(exclude_df['cid'])
    
    # we only care about titles such that the CID is not in the target_df
    # return df
# pass

def create_title_tsv():
    """
    it's just too many rows. maybe let's just stream into a tsv
    """
    exclude_df = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Synonyms-unique-CIDs.parquet')
    exclude_set = set(exclude_df['cid'])
    del exclude_df

    from tqdm import tqdm
    ctr = 0
    with open('C:/conjunct/bigdata/pubchem/CID-Title.tsv', 'w') as fout:
        with open('D:/bigdata/pubchem/CID-Title', 'r') as fin:
            for line in tqdm(fin, total=119108892):
                cid, title = line.split('\t', 1)
                if len(title) > 250:
                    continue
                    # note that the max length of a substrate is 185, 
                    # so we can be safe and exclude anything longer than 250
                if int(cid) in exclude_set:
                    continue
                fout.write(f"{cid}\t{title}")
                ctr += 1
    # 60093403 lines, 
    # 6.5 GB --> 5.587247  GB, 93 bytes per line (including bytes that represent the numerical index)
    # 
    print("Wrote", ctr, "lines")

# oops, need to add in a row
# df.r
def main():
    cids = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Synonym-filtered.parquet')
    # last CID is 172420250
    # expect 172 million rows
    
    # (103418344, 2) rows are here
    print(cids.shape)

    unique_cids = cids['cid'].unique()
    unique_cids.to_frame('cid').write_parquet('C:/conjunct/bigdata/pubchem/CID-Synonyms-unique-CIDs.parquet')
    print(len(unique_cids))

    # brendas = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')

    # # brenda_names = set(brendas['name'].str.to_lowercase().to_list())
    # # print(len(brenda_names)) # 238814
    
    # cids = cids.filter(
    #     pl.col('name').str.to_lowercase().is_in(brendas['name'].str.to_lowercase()) # brenda_names)
    # )
    # print(cids.shape) # (64272, 2)

    # print(cids.head(10))

def preview_title_tsv():
    with open('C:/conjunct/bigdata/pubchem/CID-Title.tsv', 'r') as f:
        for i, line in enumerate(f):
            if i > 10:
                break
            print(line)
if __name__ == '__main__':
    # create_title_tsv()
    # main()
    # preview_title_tsv()
    # create_title_parquet()

    df = pl.read_parquet('C:/conjunct/bigdata/pubchem/CID-Title.parquet')
    # yay we can load it into RAM!
    # print(df.head(10))
    print(df['title'].str.len_chars().mean()) # 83.6 chars