import polars as pl

from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export

@resolve
@export("data/thesaurus/enzymes/backcited.parquet")
def generate_backcited_chapter(
    thedata = REQUIRE("data/export/TheData_kcat.parquet"),
    uniprot = REQUIRE("data/enzymes/accessions/final/uniprot.parquet"),
    pdb = REQUIRE("data/enzymes/accessions/final/pdb.parquet")
):
    """
    backcited: if the **uniprot** cites one of our pmids
    """
    pmid2uniprot = uniprot.select(['uniprot', 'pmids']).explode('pmids').drop_nulls()
    pmid2uniprot = pmid2uniprot.group_by('pmids').agg(pl.col('uniprot').unique())

    doi2uni = uniprot.select(['uniprot', 'dois']).explode('dois').drop_nulls()
    doi2uni = doi2uni.group_by('dois').agg(pl.col('uniprot').unique())

    # pdb = read_all_dfs('data/enzymes/accessions/pdb', so={'pmids': pl.Utf8})
    pmid2pdb = (
        pdb.select(['pmids', 'pdb'])
        .explode('pmids')
        .rename({'pmids': 'pmid'})
        .group_by('pmid')
        .agg(pl.col('pdb').unique())
    )

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

if __name__ == '__main__':
    backcited = generate_backcited_chapter()
    print(backcited)