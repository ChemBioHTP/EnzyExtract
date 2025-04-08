
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

infos = infos.with_columns([
    pl_to_ascii(
        pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme'))
    ).alias('enzyme_preferred'),
    pl_fix_organism(pl.col('organism')).alias('organism_fixed'),
    # pl_fix_organism(pl.col('organism_right')).alias('organism_right'), # just in case
])



# Get forward cited Accessions per each PMID
cited_unscreened = pl.read_parquet('data/enzymes/sequence_scans/latest_sequence_scans.parquet')

# get rid of these

cited_unscreened = cited_unscreened.select('pmid', 'uniprot', 'refseq', 'genbank')
# complete the map from pmid to accessions
cited_unscreened = cited_unscreened.group_by('pmid').agg(
    # pl.col('pdb').drop_nulls().flatten().unique(),
    pl.col('uniprot').drop_nulls().flatten().unique(),
    pl.col('refseq').drop_nulls().flatten().unique(),
    pl.col('genbank').drop_nulls().flatten().unique(),
).select([
    'pmid', 'uniprot', 'refseq', 'genbank'
]) # should be only 1 pmid

# NOTE: some PMIDs are lost here, but that's okay
cited_unscreened = cited_unscreened.join(pmid2canonical, left_on='pmid', right_on='pmid', how='inner')

# Get backward cited accessions per each PMID
# (only available for uniprot)
backcited = pl.read_parquet('data/thesaurus/enzymes/backcited.parquet').select('canonical', 'uniprot')

# combine them
policy = 'concat_backcited'
# policy = 'delete_backcited'

if policy == 'concat_backcited':
    doc2uniprot = cited_unscreened.select('pmid', 'canonical', 'uniprot').join(
        backcited.select('canonical', 'uniprot'), left_on='canonical', right_on='canonical', how='full', suffix='_back', coalesce=True)
    doc2uniprot = doc2uniprot.with_columns([
        pl.col('uniprot').fill_null([]).list.concat(pl.col('uniprot_back').fill_null([])).alias('uniprot'),
    ])

    doc2uniprot = doc2uniprot.select([
        pl.coalesce(pl.col('uniprot_back'), pl.col('uniprot')).alias('uniprot'),
        pl.col('canonical'),
        pl.col('pmid')
    ])
elif policy == 'delete_backcited':
    doc2uniprot = cited_unscreened.select('pmid', 'canonical', 'uniprot').join(
        backcited.select('canonical', 'uniprot'), left_on='canonical', right_on='canonical', how='anti')
    doc2uniprot = doc2uniprot.select([
        pl.col('uniprot'),
        pl.col('canonical'),
        pl.col('pmid')
    ])

# filter by those still in pmid2canonical
# doc2accessions = doc2accessions.join(pmid2canonical, left_on='canonical', right_on='canonical', how='semi')

# TODO strategy: split up into doc2uniprot, doc2refseq, doc2genbank. explode each ones individually.
# join doc2uniprot, doc2refseq, doc2genbank with their respective sequences. then concat them all together 
# with how=diagonal. upon submitting, parcel them out by partitioning by index.

# join to accessions
doc2uniprot = doc2uniprot.group_by('canonical').agg(
    pl.col('uniprot').flatten().unique().drop_nulls().alias('uniprot'),

)
infos_plus_uniprot = infos.join(doc2uniprot, left_on='canonical', right_on='canonical', how='inner', validate='m:1')


### Get Uniprots
uniprot_all = pl.read_parquet('data/enzymes/accessions/final/uniprot.parquet')
uniprot_all = uniprot_all.filter(
    ~pl.col('sequence').str.contains('X')
)
uniprot_aliases = uniprot_all.filter(
    pl.col('sequence').is_not_null()
    & pl.col('uniprot_aliases').list.len() > 0
).explode('uniprot_aliases').rename({'uniprot_aliases': 'uniprot', 'uniprot': 'uniprot_aliases'}).with_columns([
    # convert to list, singleton
    pl.col('uniprot_aliases').cast(pl.List(pl.Utf8)).alias('uniprot_aliases')
]).unique('uniprot')

uniprot_all = (
    pl.concat([uniprot_all, uniprot_aliases], how='diagonal')
    .unique('uniprot', keep='first')
    .select('uniprot', 'enzyme_name', 'organism', 'organism_common', 'sequence')
)

# now do a fancy match
infos_plus_uniprot = infos_plus_uniprot.explode('uniprot')
infos_plus_uniprot = infos_plus_uniprot.join(uniprot_all, left_on='uniprot', right_on='uniprot', how='left')
infos_unmatched = infos_plus_uniprot.filter(pl.col('sequence').is_null())
infos_plus_uniprot = infos_plus_uniprot.filter(pl.col('sequence').is_not_null())

### Get NCBI: Refseqs and Genbanks
ncbi_all = pl.read_parquet('data/enzymes/accessions/final/ncbi.parquet')
ncbi_all = ncbi_all.with_row_index('_ncbi_index')
refseq_all = ncbi_all.filter(
    pl.col('ncbi').str.starts_with('NP_')
    | pl.col('ncbi').str.starts_with('YP_')
    | pl.col('ncbi').str.starts_with('XP_')
    | pl.col('ncbi').str.starts_with('WP_')
)
genbank_all = ncbi_all.join(refseq_all, left_on='ncbi', right_on='ncbi', how='anti').filter(~pl.col('ncbi').str.contains('_'))

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

# infos = pl.concat([infos_versioned, infos_unversioned], how='diagonal')
# infos = infos.unique(['index', 'pdb']) # if pdb versioned and pdb unversioned coincide. 46603 to 

# this is kind of crazy to have 10+ pdbs for one enzyme. let's just filter those out for now. 

infos_plus_uniprot = infos_plus_uniprot.select('index', 'pmid', 'canonical', 'enzyme', 'enzyme_full', 
                    'organism', 'uniprot', 'enzyme_preferred',
                     'enzyme_name', 'organism_fixed', 'organism_right', 'organism_common')

# calculate enzyme_preferred as enzyme if enzyme_full is null else enzyme_full
infos_plus_uniprot = infos_plus_uniprot.with_columns([
    # pl_to_ascii(
    #     pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme'))
    # ).alias('enzyme_preferred'),
    # pl_fix_organism(pl.col('organism')).alias('organism_fixed'),
    pl_fix_organism(pl.col('organism_right')).alias('organism_right'), # just in case
])
# calculate fuzz between these:
# organism and organism_right
# enzyme_preferred and descriptor, provided len(rhs) is larger
# enzyme_preferred and name, provided len(rhs) is larger
# enzyme_preferred and info, provided len(rhs) is larger


from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress

# Define the comparisons to calculate fuzz similarities
comparisons = [
    ('enzyme_preferred', 'enzyme_name', False, 'similarity_enzyme_name'),
    ('organism_fixed', 'organism_right', False, 'similarity_organism'),
    ('organism', 'organism_common', False, 'similarity_organism_common'),
]

# Compute the similarities with progress tracking
infos_plus_uniprot = compute_fuzz_with_progress(infos_plus_uniprot, comparisons).with_columns(
    pl.lit('uniprot').alias('accession_source'),
    pl.max_horizontal(
        pl.col(f"similarity_organism"),
        pl.col(f"similarity_organism_common"),
    ).alias('max_organism_similarity')
)

perfect_uniprot = infos_plus_uniprot.filter(
    (pl.col('max_organism_similarity') >= 95) & (pl.col('similarity_enzyme_name') >= 90)
)

perfect_uniprot = perfect_uniprot.with_columns(
    (pl.col('max_organism_similarity') + pl.col('similarity_enzyme_name')).alias('total_similarity')
).sort('total_similarity', descending=True).unique('index', keep='first')
perfect_uniprot.write_parquet('data/thesaurus/enzymes/uniprot_similar.parquet')

# write
comparisons_ncbi = [
    ('enzyme_preferred', 'descriptor', False, 'similarity_enzyme'),
    ('organism_fixed', 'descriptor', False, 'similarity_organism'),
    ('organism', 'descriptor', False, 'similarity_organism_common'),
]
infos_plus_refseq = compute_fuzz_with_progress(infos_plus_refseq, comparisons_ncbi).with_columns(
    pl.lit('refseq').alias('accession_source'),
    pl.max_horizontal(
        pl.col(f"similarity_organism"),
        pl.col(f"similarity_organism_common"),
    ).alias('max_organism_similarity'),
)

infos_plus_genbank = compute_fuzz_with_progress(infos_plus_genbank, comparisons_ncbi).with_columns(
    pl.lit('genbank').alias('accession_source'),
    pl.max_horizontal(
        pl.col(f"similarity_organism"),
        pl.col(f"similarity_organism_common"),
    ).alias('max_organism_similarity')
)

perfect_refseq = infos_plus_refseq.filter(
    (pl.col('max_organism_similarity') >= 95) & (pl.col('similarity_enzyme') >= 90)
)

perfect_genbank = infos_plus_genbank.filter(
    (pl.col('max_organism_similarity') >= 95) & (pl.col('similarity_enzyme') >= 90)
)

infos_accessions = pl.concat([infos_plus_uniprot, infos_plus_refseq, infos_plus_genbank], how='diagonal')
infos_accessions = infos_accessions.sort('total_similarity', descending=True)

namespace = 'pick-acc-prod1'
write_to = f'data/ingest/pick/{namespace}.parquet'
print("Ingest at", write_to)
info_view.write_parquet(write_to)

if use_lowercase != 'both':
    exit(0)

perfect_pdb = info_view.filter((pl.col('max_enzyme_similarity') >= 90) & (pl.col('similarity_organism') >= 95))
imperfect_pdb = info_view.filter((pl.col('max_enzyme_similarity') < 90) | (pl.col('similarity_organism') < 95))
imperfect_pdb = imperfect_pdb.join(perfect_pdb, on='index', how='anti') # perfect pdb no longer needs to be matched.
pass # 46843 to 29401

pdb_no_organism = info_view.filter((pl.col('max_enzyme_similarity') > 99) 
                                   & (pl.col('similarity_organism').is_null())
                                   & ~pl.col('index').is_in(perfect_pdb['index']))
pdb_no_organism.write_parquet(f'data/thesaurus/enzymes/pdb_similar_no_organism.parquet')
perfect_pdb.write_parquet(f'data/thesaurus/enzymes/pdb_similar.parquet')
pass




