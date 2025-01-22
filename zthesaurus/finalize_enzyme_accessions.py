import os
import polars as pl
import polars.selectors as cs
from enzyextract.thesaurus.enzyme_io import read_all_dfs
def get_total_accessions():
    """
    Step 1: Read all needed accessions

    pdb: ['pdb', 'pdb_unversioned']
    uniprot: ['uniprot']
    refseq: ['refseq']
    genbank: ['genbank']
    """
    ### Step 1: Read all needed accessions
    dfs = []
    filenames = [
        'brenda', 'scratch', 'topoff', 'wos', 'xml_abstract', 'xml'
    ]
    for filename in filenames:
        dfs.append(pl.read_parquet(f'data/enzymes/sequence_scans/{filename}_sequences.parquet'))
    df = pl.concat(dfs, how='diagonal') # type: pl.DataFrame

    pdb = df.select(['pdb']).explode('pdb').unique().drop_nulls()
    # pdb: convert to uppercase
    pdb = pdb.with_columns([
        pl.col('pdb').str.to_uppercase().alias('pdb')
    ]).unique()


    uniprot = df.select(['uniprot']).explode('uniprot').unique().drop_nulls()
    refseq = df.select(['refseq']).explode('refseq').unique().drop_nulls()
    genbank = df.select(['genbank']).explode('genbank').unique().drop_nulls()

    pdb = pdb.with_columns([
        pl.col('pdb').str.split('_').list.get(0).alias('pdb_unversioned')
    ])

    return pdb, uniprot, refseq, genbank

def get_known_uniprot_old():
    ### uniprot
    bdr = []
    for filename in os.listdir('data/enzymes/accessions/uniprot'):
        if filename.endswith('.parquet'):
            bdr.append(
                pl.scan_parquet(f'data/enzymes/accessions/uniprot/{filename}').select(
                    cs.exclude('full_response')
                ).collect()
            )
    if bdr:
        uniprot_known = pl.concat(bdr, how='diagonal')
        uniprot_known = uniprot_known.unique(['uniprot', 'sequence'])
    else:
        uniprot_known = pl.DataFrame({
            'uniprot': [],
            'enzyme': [],
            'organism': [],
            'sequence': []
        })
    return uniprot_known

def get_known_uniprot():
    """
    Note: see zthesaurus/finalize_uniprot.py
    """
    cited = pl.read_parquet('data/enzymes/accessions/uniprot/all_cited.parquet')
    searched = pl.read_parquet('data/enzymes/accessions/uniprot_searched/gen2.parquet')
    uniprot_known = pl.concat([cited, searched], how='diagonal')
    uniprot_known = uniprot_known.filter(
        pl.col('uniprot').is_not_null()
    ).unique(['uniprot', 'sequence'])
    return uniprot_known

def get_known_accessions():
    """
    Step 2: Observe known accessions
    from data/enzymes/accessions/*
    ~~formerly from fetch_sequences/results/*_fragments~~


    pdb: ['pdb', 'descriptor', 'name', 'sys_name', 'organism', 'info', 'seq', 'seq_can', 'pmids', 'dois', 'enzyme', 'pdb_unversioned']
    uniprot: ['uniprot', 'enzyme', 'organism', 'sequence']
    ncbi: ['ncbi', 'descriptor', 'sequence']
    """
    so = {'pmids': pl.Utf8}
    # pdb_known = read_all_dfs('fetch_sequences/results/pdb_fragments', so=so)
    pdb_known = read_all_dfs('data/enzymes/accessions/pdb', so=so)
    pdb_known = pdb_known.with_columns(
        pl.col('pmids').str.split('|').list.eval(
            pl.element().filter(pl.element() != '-1')
        ),
        pl.col('dois').str.split('|')
    )
    # uniprot_known = read_all_dfs('fetch_sequences/results/uniprot_fragments', so=so)
    # uniprot_known = read_all_dfs('data/enzymes/accessions/uniprot', so=so)
    uniprot_known = get_known_uniprot()
    
    # ncbi_known = read_all_dfs('fetch_sequences/results/ncbi_fragments', so=so)
    ncbi_known = read_all_dfs('data/enzymes/accessions/ncbi', so=so)
    pdb_known = pdb_known.with_columns([
        pl.col('pdb').str.split('_').list.get(0).alias('pdb_unversioned'),
        pl.col('pdb').str.split('_').list.get(1).cast(pl.UInt32).alias('pdb_version'),
    ])
    pdb_known = pdb_known.unique(['pdb', 'seq']).rename({'seq': 'sequence'})

    return pdb_known, uniprot_known, ncbi_known

def get_unknown_accessions():
    """
    Step 3: Get unknown accessions
    """

    pdb, uniprot, refseq, genbank = get_total_accessions()
    pdb_known, uniprot_known, ncbi_known = get_known_accessions()

    pdb_known = pdb_known.head(0)
    ncbi_known = ncbi_known.head(0)

    pdb_done = set(pdb_known['pdb_unversioned'])
    uniprot_done = set(uniprot_known['uniprot'])
    ncbi_done = set(ncbi_known['ncbi'])

    
    pdb_unknown = pdb.filter(
        ~pl.col('pdb_unversioned').is_in(pdb_done)
    ).select('pdb_unversioned').rename({'pdb_unversioned': 'pdb'})
    uniprot_unknown = uniprot.filter(
        ~pl.col('uniprot').is_in(uniprot_done)
    )
    refseq_unknown = refseq.filter(
        ~pl.col('refseq').is_in(ncbi_done)
    )
    genbank_unknown = genbank.filter(
        ~pl.col('genbank').is_in(ncbi_done)
    )

    print("Writing to data/enzymes/accessions/unknown/")
    pdb_unknown.write_parquet('data/enzymes/accessions/unknown/unknown_pdb.parquet')
    uniprot_unknown.write_parquet('data/enzymes/accessions/unknown/unknown_uniprot.parquet')
    refseq_unknown.write_parquet('data/enzymes/accessions/unknown/unknown_refseq.parquet')
    genbank_unknown.write_parquet('data/enzymes/accessions/unknown/unknown_genbank.parquet')

def finalize_accessions():
    pdb_known, uniprot_known, ncbi_known = get_known_accessions()
    pdb_known.write_parquet('data/enzymes/accessions/final/pdb.parquet')
    uniprot_known.write_parquet('data/enzymes/accessions/final/uniprot.parquet')
    ncbi_known.write_parquet('data/enzymes/accessions/final/ncbi.parquet')

    print("Finalized accessions")
    print("data/enzymes/accessions/final/pdb.parquet")
    print("data/enzymes/accessions/final/uniprot.parquet")
    print("data/enzymes/accessions/final/ncbi.parquet")

    # refseq_known = read_all_dfs('data/enzymes/accessions/refsesq', so=so)


if __name__ == '__main__':
    # get_unknown_accessions()
    finalize_accessions()
