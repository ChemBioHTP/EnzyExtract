import polars as pl

from enzyextract.thesaurus.enzyme_io import read_all_dfs

def generate_cited_chapter():
    df = pl.read_parquet('data/gpt/uniprot_prod1.parquet')

    uniprot_dict = df.select(['pmid', 'enzyme', 'enzyme_full', 'organism', 'best']).rename({'best': 'uniprot'})

    uniprot_seq = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet').select(['uniprot', 'sequence'])
    uniprot_seq = uniprot_seq.unique('uniprot')
    uniprot_dict = uniprot_dict.join(uniprot_seq, on='uniprot', how='left').drop_nulls(['sequence', 'uniprot'])
    uniprot_dict = uniprot_dict.with_columns([
        pl.lit('cited').alias('enzyme_source')
    ])


    # now this is just dumb. When enzyme and enzyme_full are the same, delete enzyme_full
    uniprot_dict = uniprot_dict.with_columns([
        pl.when(pl.col('enzyme') == pl.col('enzyme_full')).then(None).otherwise(pl.col('enzyme_full')).alias('enzyme_full')
    ])
    return uniprot_dict

def generate_backcited_chapter():
    """
    backcited: if the **uniprot** cites one of our pmids
    """
    thedata = pl.read_parquet('data/export/TheData_kcat.parquet')
    # uniprot = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
    # uniprot = pl.read_parquet('data/enzymes/accessions/uniprot_from_pmid/p2u_20241228-181808.parquet')
    uniprot = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
    pmid2uniprot = uniprot.select(['uniprot', 'pmids']).explode('pmids').drop_nulls()
    pmid2uniprot = pmid2uniprot.group_by('pmids').agg(pl.col('uniprot').unique())

    doi2uni = uniprot.select(['uniprot', 'dois']).explode('dois').drop_nulls()
    doi2uni = doi2uni.group_by('dois').agg(pl.col('uniprot').unique())

    so = {'pmids': pl.Utf8}
    pdb = read_all_dfs('data/enzymes/accessions/pdb', so=so)
    pmid2pdb = pdb.select(['pmids', 'pdb']).with_columns([
        pl.col('pmids').str.split('|')
    ]).explode('pmids').rename({'pmids': 'pmid'}).group_by('pmid').agg(pl.col('pdb').unique())

    backcited = thedata.select(['canonical']).unique()
    backcited = backcited.join(pmid2uniprot, left_on='canonical', right_on='pmids', how='left')
    backcited = backcited.join(pmid2pdb, left_on='canonical', right_on='pmid', how='left')

    backcited = backcited.join(doi2uni, left_on='canonical', right_on='dois', how='left', suffix='_doi')
    backcited = backcited.with_columns([
        pl.coalesce(pl.col('uniprot'), pl.col('uniprot_doi')).alias('uniprot'),
        pl.col('pdb').list.eval(
            pl.element().str.split('_').list.get(0)
        ).list.unique().alias('pdb_unversioned')
    ]).filter(pl.col('uniprot').is_not_null() | pl.col('pdb').is_not_null()).drop('uniprot_doi')
    # of 27898: 6203 have uniprot/pdb backcitation

    return backcited

from enzyextract.utils.pl_utils import wrap_pbar



if __name__ == '__main__':

    exit(0)
    thedata = pl.read_parquet('data/export/TheData.parquet')
    pmid2canonical = thedata.select(['pmid', 'canonical']).unique()

    backcited = generate_backcited_chapter()
    backcited.write_parquet('data/thesaurus/enzymes/backcited.parquet')
    exit(0)

    cited = generate_cited_chapter()
    cited = cited.join(pmid2canonical, left_on='pmid', right_on='pmid', how='left')

    # what's the difference between backcited and cited?
    pmid2uniprot_cited = cited.select(['canonical', 'uniprot']).group_by('canonical').agg(pl.col('uniprot').unique())
    # common = backcited.select('canonical', 'uniprot').join(cited.select('canonical', 'uniprot'), on='canonical', how='inner', suffix='_cited')

    those_cited_not_in_backcited = pmid2uniprot_cited.filter(
        ~pl.col('canonical').is_in(set(backcited['canonical']))
    ) # 1019 pmids

    pass

    cited.write_parquet('data/thesaurus/enzymes/uniprots_cited.parquet')

    # searched = searched