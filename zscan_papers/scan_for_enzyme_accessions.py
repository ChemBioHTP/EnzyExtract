import os
import re
from enzyextract.thesaurus.protein_patterns import pdb_pattern, pdb_pattern_stricter, uniprot_pattern, refseq_pattern, genbank_pattern, uniprot_blacklist, pdb_pattern_stricter_i, protein_data_bank_pattern
import pymupdf
from tqdm import tqdm
import polars as pl
import polars.selectors as cs


def annotate(df: pl.LazyFrame, col, keep=None):
    if keep is None:
        keep = ['pmid', 'page_number']
    
    keep.append(col)
    
    df = (
        df.select(keep)
        .with_columns([
            col.str.extract_all(pdb_pattern_stricter_i.pattern).list.unique().alias('pdb'),
            col.str.extract_all(uniprot_pattern.pattern).list.unique().alias('uniprot'),
            col.str.extract_all(refseq_pattern.pattern).list.unique().alias('refseq'),
            col.str.contains(protein_data_bank_pattern.pattern).alias('has_pdb')
        ]).with_columns([
            col.str.extract_all(genbank_pattern.pattern).list.unique() # .alias('genbank')
                .list.set_difference('uniprot').alias('genbank')
        ])
        .select(cs.exclude(col))
        # .select(keep + ['pdb', 'uniprot', 'refseq', 'genbank'])
        .with_columns([
            pl.when(pl.col('pdb').list.len() == 0)
                .then(None).otherwise(pl.col('pdb')).alias('pdb'),
            pl.when(pl.col('uniprot').list.len() == 0)
                .then(None).otherwise(pl.col('uniprot')).alias('uniprot'),
            pl.when(pl.col('refseq').list.len() == 0)
                .then(None).otherwise(pl.col('refseq')).alias('refseq'),
            pl.when(pl.col('genbank').list.len() == 0)
                .then(None).otherwise(pl.col('genbank')).alias('genbank')
        ])
        # .filter((pl.col('mutant1').list.len() > 0)
                # | (pl.col('mutant2').list.len() > 0))
        .filter(pl.col('pdb').is_not_null() | pl.col('uniprot').is_not_null() | pl.col('refseq').is_not_null() | pl.col('genbank').is_not_null())
    )
    return df

def script_look_for_protein_identifiers():
    # dfs = []

    in_df_names = ['topoff', 'scratch', 'brenda', 'wos']

    for name in in_df_names:
        print("Looking for protein idents in", name)
        df = pl.scan_parquet(f'data/scans/{name}.parquet')

        df = annotate(df, col=pl.col('text'))
        
        # df = df.collect() # .unique(maintain_order=True)
        df.sink_parquet(f'data/enzymes/sequence_scans/{name}_sequences.parquet')
        del df

def extract_goodies(text):
    # pdb_pattern_stricter: re.Pattern
    pdbs = set(pdb_pattern_stricter_i.findall(text))
    uniprots = set(uniprot_pattern.findall(text))
    refseqs = set(refseq_pattern.findall(text))
    genbanks = set(genbank_pattern.findall(text)) - uniprots
    pdbs = list(pdbs) if pdbs else None
    uniprots = list(uniprots) if uniprots else None
    refseqs = list(refseqs) if refseqs else None
    genbanks = list(genbanks) if genbanks else None

    has_pdb = bool(protein_data_bank_pattern.search(text))
    
    # if pdbs or uniprots or refseqs or genbanks:

    return {"pdb": pdbs, "uniprot": uniprots, "refseq": refseqs, "genbank": genbanks, "has_pdb": has_pdb}
    # return None

from enzyextract.utils.pl_utils import wrap_pbar

def script_look_for_protein_identifiers_xml():
    # df = pl.scan_parquet('data/scans/xml.parquet')
    # good_pmids = set(pl.read_parquet('data/valid/_valid_bucket-rebuilt.parquet')['pmid'])
    # df = df.filter(pl.col('pmid').is_in(good_pmids))
    # df.sink_parquet('data/scans/xml_slim.parquet')

    # df = pl.scan_parquet('data/scans/xml_slim.parquet')
    df = pl.read_parquet('data/scans/xml_slim.parquet').select(['pmid', 'content'])


    with tqdm(total=df.height) as pbar:

        df = df.with_columns([
            pl.col('content').map_elements(wrap_pbar(pbar, extract_goodies), return_dtype=pl.Struct({
                "pdb": pl.List(pl.Utf8),
                "uniprot": pl.List(pl.Utf8),
                "refseq": pl.List(pl.Utf8),
                "genbank": pl.List(pl.Utf8),
                "has_pdb": pl.Boolean
            })).alias('goodies')
        ]).select(cs.exclude('content'))
        df = df.filter(pl.col('goodies').is_not_null())
        df = df.unnest('goodies')
    df.write_parquet('data/enzymes/sequence_scans/xml_sequences.parquet')
    # df1.sink_parquet(f'data/enzymes/sequence_scans/xml_sequences.parquet')

def script_look_for_protein_identifiers_xml_abstract():
    df = pl.scan_parquet('data/scans/xml_slim.parquet')

    df2 = annotate(df, col=(pl.col('abstract')), keep=['pmid'])
    df2.sink_parquet(f'data/enzymes/sequence_scans/xml_abstract_sequences.parquet')

    
    # print(df.head(10).collect())

def consolidate_sequence_scans():
    from enzyextract.thesaurus.enzyme_io import read_all_dfs
    def blacklist(x):
        return 'xml_sequences' in x or 'latest_sequence_scans' in x
    df = read_all_dfs('data/enzymes/sequence_scans', blacklist=blacklist)
    df2 = pl.read_parquet('data/enzymes/sequence_scans/xml_sequences.parquet')
    df2 = df2.rename({
        'pdbs': 'pdb',
        'uniprots': 'uniprot',
        'refseqs': 'refseq',
        'genbanks': 'genbank'
    }, strict=False)
    df = pl.concat([df, df2], how='diagonal')
    df.write_parquet('data/enzymes/sequence_scans/latest_sequence_scans.parquet')


if __name__ == "__main__":
    
    # script_look_for_protein_identifiers()
    # script_look_for_protein_identifiers_xml()
    # script_look_for_protein_identifiers_xml_abstract()

    consolidate_sequence_scans()