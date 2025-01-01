
import polars as pl
from tqdm import tqdm
from enzyextract.prompts.ask_best_uniprots import pick_accessions, PickAccessionSchemaAdditional
from enzyextract.submit.batch_utils import chunked_write_to_jsonl
from enzyextract.submit.openai_management import submit_batch_file
from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.organism_patterns import pl_fix_organism

# df = pl.read_parquet('data/valid/_valid_with_duplicates.parquet')
df = pl.read_parquet('data/export/TheData_kcat.parquet')

pmid2canonical = pl.scan_parquet('data/export/TheData.parquet').select(['pmid', 'canonical']).unique().collect()

infos = df.select([
    'pmid',
    'enzyme',
    'enzyme_full',
    'organism',
    'canonical',
]).unique()

infos = infos.filter(
    pl.col('enzyme').is_not_null()
    | pl.col('enzyme_full').is_not_null()
).with_row_index('index')


### Load organism thesaurus
organism_df = pl.read_parquet('data/thesaurus/organism/uniprot_organism.parquet').drop_nulls()
organism_df = (
    organism_df
    .with_columns([
        pl.col('organism_common').str.to_lowercase().alias('organism_common'),
    ]).sort('frequency', descending=True)
    .unique('organism_common', keep='first')
    .select(['organism_common', 'organism'])
    .rename({'organism': 'organism_uniprot'})
)
### Fix organisms
infos = (
    # 1. use uniprot organism thesaurus. create column organism_uniprot
    infos.with_columns([
        pl.col('organism').str.to_lowercase().alias('organism_lower'),
    ])
    .join(organism_df, left_on='organism_lower', right_on='organism_common', how='left', validate='m:1')

    # 2. use manually written corrections on dictionary.
    .with_columns([
        pl_to_ascii(
            pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme'))
        ).alias('enzyme_preferred'),
        pl_fix_organism(pl.col('organism')).alias('organism_fixed'),
        # pl_fix_organism(pl.col('organism_right')).alias('organism_right'), # just in case
    ])
    .drop('organism_lower')
)



# Get forward cited Accessions per each PMID
cited_unscreened = pl.read_parquet('data/enzymes/sequence_scans/latest_sequence_scans.parquet')
cited_unscreened = cited_unscreened.select('pmid', 'refseq', 'genbank')
cited_unscreened = cited_unscreened.group_by('pmid').agg(
    # pl.col('pdb').drop_nulls().flatten().unique(),
    # pl.col('uniprot').drop_nulls().flatten().unique(),
    pl.col('refseq').drop_nulls().flatten().unique(),
    pl.col('genbank').drop_nulls().flatten().unique(),
).select([
    'pmid', 'refseq', 'genbank'
]) # should be only 1 pmid

# NOTE: some PMIDs are lost here, but that's okay
cited_unscreened = cited_unscreened.join(pmid2canonical, left_on='pmid', right_on='pmid', how='inner')

# backcited only available for uniprot
# backcited = pl.read_parquet('data/enzymes/thesaurus/backcited.parquet').select('canonical', 'uniprot')

### Get NCBI: Refseqs and Genbanks
ncbi_all = pl.read_parquet('data/enzymes/accessions/final/ncbi.parquet')
# ncbi_all = ncbi_all.with_row_index('_ncbi_index')
refseq_all = ncbi_all.filter(
    pl.col('ncbi').str.starts_with('NP_')
    | pl.col('ncbi').str.starts_with('YP_')
    | pl.col('ncbi').str.starts_with('XP_')
    | pl.col('ncbi').str.starts_with('WP_')
)
genbank_all = ncbi_all.join(refseq_all, left_on='ncbi', right_on='ncbi', how='anti').filter(~pl.col('ncbi').str.contains('_'))
# genbank_all = genbank_all.filter(
#     pl.col('sequence').str.contains('[BD-FH-SV-Z]') # is not a DNA or RNA sequence
# )

### do the same for refseq
doc2refseq = cited_unscreened.select('pmid', 'canonical', 'refseq').explode('refseq').drop_nulls()
doc2refseq = doc2refseq.group_by('canonical').agg(
    pl.col('refseq').flatten().unique().drop_nulls().alias('refseq'),
)

infos_plus_refseq = infos.join(doc2refseq, left_on='canonical', right_on='canonical', how='inner', validate='m:1')
infos_plus_refseq = infos_plus_refseq.explode('refseq')
infos_plus_refseq = infos_plus_refseq.join(refseq_all, left_on='refseq', right_on='ncbi', how='inner')

### do the same for genbank
doc2genbank = cited_unscreened.select('pmid', 'canonical', 'genbank').explode('genbank').drop_nulls()
doc2genbank = doc2genbank.group_by('canonical').agg(
    pl.col('genbank').flatten().unique().drop_nulls().alias('genbank'),
)

infos_plus_genbank = infos.join(doc2genbank, left_on='canonical', right_on='canonical', how='inner', validate='m:1')
infos_plus_genbank = infos_plus_genbank.explode('genbank')
infos_plus_genbank = infos_plus_genbank.join(genbank_all, left_on='genbank', right_on='ncbi', how='inner')

infos_plus_refseq = infos_plus_refseq.rename({'refseq': 'ncbi'})
infos_plus_genbank = infos_plus_genbank.rename({'genbank': 'ncbi'})

info_view = pl.concat([infos_plus_refseq, infos_plus_genbank], how='diagonal')


from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress

# Compute the similarities with progress tracking
comparisons_ncbi = [
    ('enzyme_preferred', 'descriptor', False, 'similarity_enzyme'),
    ('organism_fixed', 'descriptor', False, 'similarity_organism_simple'),
    ('organism', 'descriptor', False, 'similarity_organism_common'),
    ('organism_uniprot', 'descriptor', False, 'similarity_organism_uniprot'),
]
infos_plus_ncbi = compute_fuzz_with_progress(info_view, comparisons_ncbi).with_columns(
    pl.max_horizontal(
        pl.col(f"similarity_organism_simple"),
        pl.col(f"similarity_organism_common"),
        pl.col(f"similarity_organism_uniprot")
    ).alias('max_organism_similarity'),
).rename({
    'similarity_enzyme': 'max_enzyme_similarity',
    # 'similarity_organism_simple': 'max_organism_similarity',
})

perfect_ncbi = infos_plus_ncbi.filter(
    (pl.col('max_organism_similarity') >= 95) & (pl.col('max_enzyme_similarity') >= 90)
)

# infos_accessions = pl.concat([infos_plus_refseq, infos_plus_genbank], how='diagonal')

namespace = 'pick-ncbi-prod1'
write_to = f'data/ingest/pick/{namespace}.parquet'
print("Ingest at", write_to)
infos_plus_ncbi.write_parquet(write_to)

perfect_ncbi = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') >= 90) & (pl.col('max_organism_similarity') >= 95))
imperfect_ncbi = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') < 90) | (pl.col('max_organism_similarity') < 95))
imperfect_ncbi = imperfect_ncbi.join(perfect_ncbi, on='index', how='anti') # perfect pdb no longer needs to be matched.
pass # 46843 to 29401

pdb_no_organism = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') > 99) 
                                   & (pl.col('max_organism_similarity').is_null())
                                   & ~pl.col('index').is_in(perfect_ncbi['index']))
pdb_no_organism.write_parquet(f'data/enzymes/thesaurus/ncbi_similar_no_organism.parquet')


perfect_ncbi = perfect_ncbi.with_columns(
    (pl.col('max_organism_similarity') + pl.col('max_enzyme_similarity')).alias('total_similarity')
)
perfect_ncbi.write_parquet(f'data/enzymes/thesaurus/ncbi_similar.parquet')
print("Similars at data/enzymes/thesaurus/ncbi_similar.parquet")
pass




