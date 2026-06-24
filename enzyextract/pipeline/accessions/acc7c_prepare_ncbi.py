
import polars as pl
from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress
from enzyextract.thesaurus.organism_patterns import pl_fix_organism

@resolve
def script_prepare_possible_ncbi(
    df = REQUIRE("data/export/TheData_kcat.parquet"),
    manifest = REQUIRE("data/manifest.parquet"),
    sequence_scans_df = REQUIRE("data/enzymes/sequence_scans/latest_sequence_scans.parquet"),

    ncbi_all = REQUIRE("data/enzymes/accessions/final/ncbi.parquet"),
    organism_df = REQUIRE("data/thesaurus/organism/uniprot_organism.parquet"),
):
    """
    Prepare a dataframe of NCBI accessions, with their associated enzyme names and organisms.
    The most relevant accession is selected based on heuristics and LLM-based fuzzy matching.
    """

    pmid2canonical = manifest.select("pmid", "canonical").unique(keep="first", maintain_order=True)

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
    organism_df = organism_df.drop_nulls()
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
    sequence_scans_df = sequence_scans_df.select('pmid', 'refseq', 'genbank')
    sequence_scans_df = sequence_scans_df.group_by('pmid').agg(
        pl.col('refseq').drop_nulls().flatten().unique(),
        pl.col('genbank').drop_nulls().flatten().unique(),
    ).select([
        'pmid', 'refseq', 'genbank'
    ]) # should be only 1 pmid

    # NOTE: some PMIDs are lost here, but that's okay
    sequence_scans_df = sequence_scans_df.join(pmid2canonical, left_on='pmid', right_on='pmid', how='inner')

    # backcited only available for uniprot and pdb

    ### Get NCBI: Refseqs and Genbanks
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
    doc2refseq = sequence_scans_df.select('pmid', 'canonical', 'refseq').explode('refseq').drop_nulls()
    doc2refseq = doc2refseq.group_by('canonical').agg(
        pl.col('refseq').flatten().unique().drop_nulls().alias('refseq'),
    )

    infos_plus_refseq = infos.join(doc2refseq, left_on='canonical', right_on='canonical', how='inner', validate='m:1')
    infos_plus_refseq = infos_plus_refseq.explode('refseq')
    infos_plus_refseq = infos_plus_refseq.join(refseq_all, left_on='refseq', right_on='ncbi', how='inner')

    ### do the same for genbank
    doc2genbank = sequence_scans_df.select('pmid', 'canonical', 'genbank').explode('genbank').drop_nulls()
    doc2genbank = doc2genbank.group_by('canonical').agg(
        pl.col('genbank').flatten().unique().drop_nulls().alias('genbank'),
    )

    infos_plus_genbank = infos.join(doc2genbank, left_on='canonical', right_on='canonical', how='inner', validate='m:1')
    infos_plus_genbank = infos_plus_genbank.explode('genbank')
    infos_plus_genbank = infos_plus_genbank.join(genbank_all, left_on='genbank', right_on='ncbi', how='inner')

    infos_plus_refseq = infos_plus_refseq.rename({'refseq': 'ncbi'})
    infos_plus_genbank = infos_plus_genbank.rename({'genbank': 'ncbi'})

    info_view = pl.concat([infos_plus_refseq, infos_plus_genbank], how='diagonal')


    # Compute the similarities with progress tracking
    comparisons_ncbi = [
        ('enzyme_preferred', 'descriptor', False, 'similarity_enzyme'),
        ('organism_fixed', 'descriptor', False, 'similarity_organism_simple'),
        ('organism', 'descriptor', False, 'similarity_organism_common'),
        ('organism_uniprot', 'descriptor', False, 'similarity_organism_uniprot'),
    ]
    infos_plus_ncbi = compute_fuzz_with_progress(info_view, comparisons_ncbi).with_columns(
        pl.max_horizontal(
            pl.col("similarity_organism_simple"),
            pl.col("similarity_organism_common"),
            pl.col("similarity_organism_uniprot")
        ).alias('max_organism_similarity'),
    ).rename({
        'similarity_enzyme': 'max_enzyme_similarity',
        # 'similarity_organism_simple': 'max_organism_similarity',
    })



    if False:
        perfect_ncbi = infos_plus_ncbi.filter(
            (pl.col('max_organism_similarity') >= 95) & (pl.col('max_enzyme_similarity') >= 90)
        )
        perfect_ncbi = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') >= 90) & (pl.col('max_organism_similarity') >= 95))
        imperfect_ncbi = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') < 90) | (pl.col('max_organism_similarity') < 95))
        imperfect_ncbi = imperfect_ncbi.join(perfect_ncbi, on='index', how='anti') # perfect pdb no longer needs to be matched.
        pass # 46843 to 29401

        ncbi_no_organism = infos_plus_ncbi.filter((pl.col('max_enzyme_similarity') > 99) 
                                        & (pl.col('max_organism_similarity').is_null())
                                        & ~pl.col('index').is_in(perfect_ncbi['index']))
        ncbi_no_organism.write_parquet('data/thesaurus/enzymes/ncbi_similar_no_organism.parquet')


        perfect_ncbi = perfect_ncbi.with_columns(
            (pl.col('max_organism_similarity') + pl.col('max_enzyme_similarity')).alias('total_similarity')
        )
        perfect_ncbi.write_parquet('data/thesaurus/enzymes/ncbi_similar.parquet')
        print("Similars at data/thesaurus/enzymes/ncbi_similar.parquet")

    write_to = 'data/enzymes/pick/pick-ncbi-v1.parquet'
    print("Ingest at", write_to)
    infos_plus_ncbi.write_parquet(write_to)
    return infos_plus_ncbi


if __name__ == '__main__':
    script_prepare_possible_ncbi()
