# Written Dec 25 2024

import polars as pl
import polars.selectors as cs

replace_greek = {'α': 'alpha', 'β': 'beta', 'ß': 'beta',
                 'γ': 'gamma', 'δ': 'delta', 'ε': 'epsilon', 'ζ': 'zeta', 'η': 'eta', 'θ': 'theta', 'ι': 'iota', 'κ': 'kappa', 'λ': 'lambda', 'μ': 'mu', 'ν': 'nu', 'ξ': 'xi', 'ο': 'omicron', 'π': 'pi', 'ρ': 'rho', 'σ': 'sigma', 'τ': 'tau', 'υ': 'upsilon', 'φ': 'phi', 'χ': 'chi', 'ψ': 'psi', 'ω': 'omega'}
replace_nonascii = {'\u2018': "'", '\u2019': "'", '\u2032': "'", '\u201c': '"', '\u201d': '"', '\u2033': '"',
                    '\u00b2': '2', '\u00b3': '3',
                    '\u207a': '+', # '\u207b': '-', # '\u207c': '=', '\u207d': '(', '\u207e': ')',
                    '(±)-': '', # we cannot handle the ± character anyways
                    '®': '',
                    }

import gzip
import csv
from tqdm import tqdm

def produce_cid_to_title(lowercase_names: set[str]):
    """
    load up substrate idents. 
    produce subset of CID that is of interest.

    lowercase_names SHOULD BE ascii.
    """
    print("Regenerating cid2title (will cache)")


    write_dest = 'data/substrates/cid/cid2title.tsv'

    # Define the path to your gzipped TSV file
    title_tsv = 'data/substrates/cid/CID-Title.gz'
    # Define the condition function to filter rows


    # Stream read the file and filter rows
    with open(write_dest, 'w') as sink:
        sink.write("CID\tName\n")
        with gzip.open(title_tsv, 'rt', encoding='utf-8', errors='replace') as gzfile:
            reader = csv.reader(gzfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in tqdm(reader):
                # 1st element is the CID, 2nd element is the title
                try:
                    cid = row[0]
                    title = row[1]
                    if title.lower() in lowercase_names:
                        sink.write(f"{cid}\t{title}\n")
                except IndexError:
                    print("Malformed row:", row)



def produce_cid_to_synonym(lowercase_names: set[str]):
    """
    load up substrate idents. 
    produce subset of CID that is of interest.

    lowercase_names SHOULD BE ascii.
    """
    print("Regenerating cid2synonym (will cache)")


    write_dest = 'data/substrates/cid/cid2synonym.tsv'

    # Define the path to your gzipped TSV file
    synonym_tsv = 'data/substrates/cid/CID-Synonym-filtered.gz'
    # Stream read the file and filter rows
    with open(write_dest, 'w') as sink:
        sink.write("CID\tName\n")
        with gzip.open(synonym_tsv, 'rt', encoding='utf-8', errors='replace') as gzfile:
            reader = csv.reader(gzfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row in tqdm(reader):
                # 1st element is the CID, 2nd element is the title
                try:
                    cid = row[0]
                    title = row[1]
                    if title.lower() in lowercase_names:
                        sink.write(f"{cid}\t{title}\n")
                except IndexError:
                    print("Malformed row:", row)

def extracted_substrate_names():
    # df = pl.read_parquet('data/valid/_valid_everything.parquet')
    df = pl.read_parquet('data/valid/_valid_with_duplicates.parquet')

    # take substrate and substrate_full columns
    substrates = df['substrate']
    # concat substrate_full
    substrates_full = df['substrate_full']
    # take the unique values
    want = pl.concat([substrates, substrates_full]).unique().drop_nulls()
    # lowercase them

    want_df = want.to_frame('name').with_columns([
        pl.col('name')
        .str.to_lowercase()
        .str.replace_all('[-᠆‑‒–—―﹘﹣－˗−‐⁻]', '-')
        .str.replace_many(replace_greek)
        .str.replace_many(replace_nonascii)
        .alias('name_lower'),
    ]).with_columns([
        (pl.col('name_lower').str.find('[^ -~]').is_not_null()).alias('not_ascii')
    ])
    want_names = set(want_df['name_lower'])
    return want_names

def extracted_brenda_names():
    df = pl.read_parquet('data/substrates/brenda_inchi_all.parquet')
    # print(df.columns)
    want_names = set(df['name'].str.to_lowercase())
    return want_names
if __name__ == '__main__':
    
    want_names = extracted_substrate_names() | extracted_brenda_names()
    print("Processing: ", len(want_names), "names")
    produce_cid_to_title(want_names)
    produce_cid_to_synonym(want_names)