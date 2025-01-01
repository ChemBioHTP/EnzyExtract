import polars as pl

uniprot_all = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
uniprot_all = uniprot_all.filter(
    ~pl.col('sequence').str.contains('X')
)

# uniprot_organism_thesaurus = uniprot_all.select(
#     pl.col('organism'),
#     pl.col('organism_common'),
#     # also add a frequency
# ).unique().write_parquet('data/thesaurus/organism/uniprot_organism.parquet')

uniprot_organism_thesaurus = (
    uniprot_all
    .group_by(['organism', 'organism_common'])
    .agg(pl.len().alias('frequency'))
    .sort('frequency', descending=True)
)

uniprot_organism_thesaurus.write_parquet('data/thesaurus/organism/uniprot_organism.parquet')

