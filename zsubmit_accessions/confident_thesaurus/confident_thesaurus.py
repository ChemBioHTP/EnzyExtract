
import re
import polars as pl
import polars.selectors as cs
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.organism_patterns import pl_fix_organism
from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress

pmid2canonical = pl.scan_parquet('data/export/TheData.parquet').select(['pmid', 'canonical']).unique().collect()

def get_wanted_names():
    """
    Get the tuples in question for which we want enzyme sequences.
    """

    df = pl.read_parquet('data/export/TheData_kcat.parquet')

    

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
        pl_fix_organism(pl.col('organism')).alias('organism_scientific_manual'),
        # pl_fix_organism(pl.col('organism_right')).alias('organism_right'), # just in case
    ]).with_columns(
        pl.when(
            pl.col('organism_scientific_manual') == pl.col('organism')
        ).then(None).otherwise(pl.col('organism_scientific_manual')).alias('organism_scientific_manual')
    )

    ### Load organism thesaurus
    organism_df = pl.read_parquet('data/thesaurus/organism/uniprot_organism.parquet').drop_nulls()
    organism_df = (
        organism_df
        .with_columns([
            pl.col('organism_common').str.to_lowercase().alias('organism_common'),
        ]).sort('frequency', descending=True)
        .unique('organism_common', keep='first')
        .select(['organism_common', 'organism'])
        .rename({'organism': 'organism_scientific_uniprot'})
    )

    ### Convert organisms to scientific names
    def remove_organism_from_name(x):
        e = x['enzyme_preferred']
        o = x['organism']
        os = x['organism_scientific']
        if o:
            e = re.sub(re.escape(o), '', e, flags=re.IGNORECASE).strip()
        if os:
            e = re.sub(re.escape(os), '', e, flags=re.IGNORECASE).strip()
        return e
        
    infos = (
        # 1. use uniprot organism thesaurus. create column organism_scientific
        infos.with_columns([
            pl.col('organism').str.to_lowercase().alias('organism_lower'),
        ])
        .join(organism_df, left_on='organism_lower', right_on='organism_common', how='left', validate='m:1')

        # 2. use manually written corrections on dictionary.
        .with_columns([
            pl_to_ascii(
                pl.coalesce(pl.col('enzyme_full'), pl.col('enzyme'))
            ).alias('enzyme_preferred'),
            # pl_fix_organism(pl.col('organism')).alias('organism_fixed'),
            # pl_fix_organism(pl.col('organism_right')).alias('organism_right'), # just in case
            pl.coalesce([
                pl.col('organism_scientific_manual'),
                pl.col('organism_scientific_uniprot')
            ]).alias('organism_scientific')
        ]).with_columns(
            pl.struct('enzyme_preferred', 'organism', 'organism_scientific').map_elements(
                remove_organism_from_name, return_dtype=pl.Utf8
            )
        )
        .drop('organism_lower')
        .drop('organism_scientific_manual', 'organism_scientific_uniprot')
    )

    # columns: pmid, enzyme, enzyme_full, organism, canonical, enzyme_preferred, organism_fixed, organism_scientific
    return infos





### Get forward cited Accessions per each PMID
def forward_cites():
    """
    Returns 3 dataframes: uniprot, pdb, ncbi
    Each dataframe has columns: 
    - pmid, canonical, uniprot
    - pmid, canonical, pdb
    - pmid, canonical, refseq, genbank
    
    """
    cited_unscreened = pl.read_parquet('data/enzymes/sequence_scans/latest_sequence_scans.parquet')

    # NOTE: some PMIDs are lost here, but that's okay
    # (we only want kinetic PMIDs)
    cited_unscreened = cited_unscreened.join(pmid2canonical, left_on='pmid', right_on='pmid', how='inner')

    uniprot = cited_unscreened.select('canonical', 'uniprot') # , 'refseq', 'genbank')
    uniprot = uniprot.filter(
        pl.col('uniprot').is_not_null()
    ).group_by('canonical').agg(
        pl.col('uniprot').flatten().unique(),
    ).select([
        'canonical', 'uniprot',
    ]) # should be only 1 pmid

    pdb = cited_unscreened.select('canonical', 'pdb', 'has_pdb').filter(
        pl.col('has_pdb')
        # require that PDB can't have both uppercase and lowercase (eliminates "mL" and "mM")
        
    )
    pdb = pdb.filter(
        pl.col('pdb').is_not_null()
    ).group_by('canonical').agg(
        pl.col('pdb').drop_nulls().flatten().unique(),
    ).select([
        'canonical', 'pdb',
    ])

    ncbi = cited_unscreened.select('canonical', 'refseq', 'genbank')
    ncbi = ncbi.filter(
        pl.col('refseq').is_not_null()
    ).group_by('canonical').agg(
        pl.col('refseq').drop_nulls().flatten().unique(),
        pl.col('genbank').drop_nulls().flatten().unique(),
    ).select([
        'canonical', 'refseq', 'genbank'
    ])
    return uniprot, pdb, ncbi


    
    


def backward_cites():
    """
    Get backward cited accessions per each PMID
    (only available for uniprot and PDB)

    - canonical, uniprot
    - canonical, pdb_versioned
    
    """
    
    # Get backward cited accessions per each PMID
    # (only available for uniprot and PDB)
    backcited = pl.read_parquet('data/enzymes/thesaurus/backcited.parquet') # .select('canonical', 'uniprot')

    uniprot = backcited.select('canonical', 'uniprot').filter(
        pl.col('uniprot').is_not_null()
    )
    pdb = backcited.select('canonical', 'pdb').filter(
        pl.col('pdb').is_not_null()
    ).rename({
        'pdb': 'pdb_versioned'
    })
    return uniprot, pdb

def forward_searches():
    """
    Get searched
    """
    pass

def get_all_cited():
    """
    Get all cited
    """
    f_uniprot, f_pdb, f_ncbi = forward_cites()
    b_uniprot, b_pdb = backward_cites()
    f_uniprot = f_uniprot.explode('uniprot').with_columns(
        pl.lit(True).alias('forward_cite')
    )
    f_pdb = f_pdb.explode('pdb').with_columns(
        pl.lit(True).alias('forward_cite'),
        pl.col('pdb').str.to_uppercase().alias('pdb_common')
    ).filter(
        ~(pl.col('pdb').str.contains(r'[a-z]') & pl.col('pdb').str.contains(r'[A-Z]'))
    )

    # split refseq and genbank, then explode, concat, add forward_cite
    f_refseq = f_ncbi.select('canonical', 'refseq').explode('refseq').rename({
        'refseq': 'ncbi'
    }).with_columns(
        pl.lit(True).alias('forward_cite'),
        pl.lit(True).alias('is_refseq')
    ).unique(['canonical', 'ncbi'], maintain_order=True)
    f_genbank = f_ncbi.select('canonical', 'genbank').explode('genbank').rename({
        'genbank': 'ncbi'
    }).with_columns(
        pl.lit(True).alias('forward_cite'),
        pl.lit(False).alias('is_refseq')
    ).unique(['canonical', 'ncbi'], maintain_order=True)

    f_ncbi = pl.concat([f_refseq, f_genbank]) # , how='diagonal')

    b_uniprot = b_uniprot.explode('uniprot').with_columns(
        pl.lit(True).alias('backward_cite')
    )
    b_pdb = b_pdb.explode('pdb_versioned').with_columns(
        pl.lit(True).alias('backward_cite'),
        pl.col('pdb_versioned').str.split('_').list.get(0).alias('pdb_common')
    )

    uniprot = f_uniprot.join(b_uniprot, left_on=['canonical', 'uniprot'], right_on=['canonical', 'uniprot'], how='full', coalesce=True).with_columns(
        pl.col('forward_cite').fill_null(False).alias('forward_cite'),
        pl.col('backward_cite').fill_null(False).alias('backward_cite')
    )
    pdb = f_pdb.join(b_pdb, left_on=['canonical', 'pdb_common'], right_on=['canonical', 'pdb_common'], how='full', coalesce=True).with_columns(
        pl.col('forward_cite').fill_null(False).alias('forward_cite'),
        pl.col('backward_cite').fill_null(False).alias('backward_cite')
    )
    ncbi = f_ncbi

    pdb = pdb.rename({
        'pdb': 'pdb_raw' # avoid confusion
    })

    return uniprot, pdb, ncbi

def get_sequence_descriptions():
    """
    Get descriptions for each enzyme accession

    Returns 4 dataframes: uniprot, pdb, ncbi, (pdb_latest)
    """
    pdb_all = pl.read_parquet('data/enzymes/accessions/final/pdb.parquet')
    pdb_all = pdb_all.filter(
        ~pl.col('seq_can').str.contains('X')
    )

    # perform a rename
    renames = {
        'descriptor': 'PDB_descriptor',
        'name': 'PDB_name',
        'sys_name': 'PDB_sys_name',
        'organism': 'PDB_organism',
        'info': 'PDB_info',
        # 'sequence': 'PDB_sequence',
        'seq_can': 'PDB_seq_can',
        # 'pmids': 'PDB_pmids',
        # 'dois': 'PDB_dois',
        # 'pdb',
        # 'pdb_version',
    }
    pdb_all = pdb_all.rename(renames).select(
        cs.exclude('pmids', 'dois', 'enzyme')
    )
    pdb_latest = (
        pdb_all.sort('pdb_version', descending=True)
            .unique('pdb_unversioned', keep='first')
            .select('pdb_version', 'pdb_unversioned', 'pdb')
    ).rename({'pdb': 'pdb_versioned'})

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
        .select('uniprot', 'enzyme_name', 'organism', 'organism_common', 'sequence',
                'recommended_name', 'submission_names', 'alternative_names')
    ).rename({
        'enzyme_name': 'UNIPROT_enzyme_name',
        'organism': 'UNIPROT_organism',
        'organism_common': 'UNIPROT_organism_common',
        # 'sequence': 'UNIPROT_sequence',
        'recommended_name': 'UNIPROT_recommended_name',
        'submission_names': 'UNIPROT_submission_names',
        'alternative_names': 'UNIPROT_alternative_names'
    })

    ### Get NCBI: Refseqs and Genbanks
    ### Get NCBI: Refseqs and Genbanks
    ncbi_all = pl.read_parquet('data/enzymes/accessions/final/ncbi.parquet')
    ncbi_all = ncbi_all.rename({
        # 'ncbi',
        'descriptor': 'NCBI_descriptor',
        # 'sequence': 'NCBI_sequence',
    })

    return uniprot_all, pdb_all, ncbi_all, pdb_latest
    # ncbi_all = ncbi_all.with_row_index('_ncbi_index')
    refseq_all = ncbi_all.filter(
        pl.col('ncbi').str.starts_with('NP_')
        | pl.col('ncbi').str.starts_with('YP_')
        | pl.col('ncbi').str.starts_with('XP_')
        | pl.col('ncbi').str.starts_with('WP_')
    )
    genbank_all = ncbi_all.join(refseq_all, left_on='ncbi', right_on='ncbi', how='anti').filter(~pl.col('ncbi').str.contains('_'))

def add_descriptions(u, p, n, udesc, pdesc, ndesc):
    """
    Add descriptions to each accession dataframe
    """
    uall = u.join(udesc, left_on='uniprot', right_on='uniprot', how='left', coalesce=True)
    pall = p.join(pdesc, left_on='pdb_versioned', right_on='pdb', how='left', coalesce=True)
    nall = n.join(ndesc, left_on='ncbi', right_on='ncbi', how='left', coalesce=True)

    return uall, pall, nall

def join_wanted(names, u, p, n):
    """
    Join the names with the cited accessions by canonical
    """

    # Based on document id, join all possible names of {enzymes, organisms} to our citations
    u = u.join(names, left_on='canonical', right_on='canonical', how='inner')
    p = p.join(names, left_on='canonical', right_on='canonical', how='inner')
    n = n.join(names, left_on='canonical', right_on='canonical', how='inner')

    return u, p, n
    pass


def perform_fuzz(uall, pall, nall):
    """
    Perform fuzz comparisons between the names and the descriptions

    uall, pall, nall should have the query joined (join_wanted) and the descriptions added (add_descriptions)
    """

    # note: we only need to calculate fuzzing for each pair once.

    

    ### PDB
    PDB_comparisons = [
        ('organism', 'PDB_organism', False, 'similarity_organism_common'),
        ('organism_scientific', 'PDB_organism', False, 'similarity_organism_scientific'),
        ('enzyme_preferred', 'PDB_descriptor', False, 'similarity_enzyme_vs_descriptor'),
        ('enzyme_preferred', 'PDB_name', False, 'similarity_enzyme_vs_name'),
        ('enzyme_preferred', 'PDB_info', False, 'similarity_enzyme_vs_info')
    ]

    pneeded = pall.select([
        'organism', 'organism_scientific', 'enzyme_preferred', 'pdb_versioned', 'PDB_organism', 'PDB_descriptor', 'PDB_name', 'PDB_info'
    ]).unique(['organism', 'enzyme_preferred', 'pdb_versioned'])
    pneeded = compute_fuzz_with_progress(pneeded, PDB_comparisons).with_columns(
        pl.max_horizontal(
            pl.col(f"similarity_enzyme_vs_descriptor"),
            pl.col(f"similarity_enzyme_vs_name"),
            pl.col(f"similarity_enzyme_vs_info")
        ).alias('max_enzyme_similarity'),
        pl.max_horizontal(
            pl.col(f"similarity_organism_common"),
            pl.col(f"similarity_organism_scientific")
        ).alias('max_organism_similarity')
    ).select(
        cs.starts_with('similarity_'),
        cs.starts_with('max_'),
        'organism', 'enzyme_preferred', 'pdb_versioned'
    )

    # we can easily join this back to the original dataframe, solely based on the 
    # independent variables (organism, enzyme_preferred, pdb_versioned)
    pall = pall.join(pneeded, on=['organism', 'enzyme_preferred', 'pdb_versioned'], how='left', join_nulls=True)


    ### UniProt
    UNIPROT_comparisons = [
        # ('enzyme_preferred', 'enzyme_name', False, 'similarity_enzyme_name'),
        ('organism', 'UNIPROT_organism', False, 'similarity_organism_plain'),
        ('organism', 'UNIPROT_organism_common', False, 'similarity_organism_common'),
        ('organism_scientific', 'UNIPROT_organism', False, 'similarity_organism_scientific'), 
        ('organism_scientific', 'UNIPROT_organism_common', False, 'similarity_organism_cross'),
        # NOTE that organism_scientific <=> UNIPROT_organism_common is nearly pointless, because 
        # then, organism would automatically match UNIPROT_organism_common
        # only exception are select hardcoded renames.
        ('enzyme_preferred', 'UNIPROT_enzyme_name', False, 'similarity_enzyme_vs_name'),
        ('enzyme_preferred', 'UNIPROT_recommended_name', False, 'similarity_enzyme_vs_recommended'),
        ('enzyme_preferred', 'UNIPROT_submission_names', False, 'similarity_enzyme_vs_submission'),
        ('enzyme_preferred', 'UNIPROT_alternative_names', False, 'similarity_enzyme_vs_alternative'),
    ]

    uneeded = uall.select([
        'organism', 'organism_scientific', 'enzyme_preferred', 'uniprot', 
        'UNIPROT_enzyme_name', 'UNIPROT_organism', 'UNIPROT_organism_common',
        'UNIPROT_recommended_name', 'UNIPROT_submission_names', 'UNIPROT_alternative_names'
    ]).unique(['organism', 'enzyme_preferred', 'uniprot'])
    uneeded = compute_fuzz_with_progress(uneeded, UNIPROT_comparisons).with_columns([
        pl.max_horizontal(
            pl.col(f"similarity_organism_plain"),
            pl.col(f"similarity_organism_common"),
            pl.col(f"similarity_organism_scientific"),
            pl.col(f"similarity_organism_cross")
        ).alias('max_organism_similarity'),
        pl.max_horizontal(
            pl.col(f"similarity_enzyme_vs_name"),
            pl.col(f"similarity_enzyme_vs_recommended"),
            pl.col(f"similarity_enzyme_vs_submission"),
            pl.col(f"similarity_enzyme_vs_alternative")
        ).alias('max_enzyme_similarity'),
    ]).select(
        cs.starts_with('similarity_'),
        cs.starts_with('max_'),
        'organism', 'enzyme_preferred', 'uniprot'
    )
    uall = uall.join(uneeded, on=['organism', 'enzyme_preferred', 'uniprot'], how='left', join_nulls=True)

    ### NCBI
    NCBI_comparisons = [
        ('enzyme_preferred', 'NCBI_descriptor', False, 'max_enzyme_similarity'),
        ('organism', 'NCBI_descriptor', False, 'similarity_organism_common'),
        ('organism_scientific', 'NCBI_descriptor', False, 'similarity_organism_scientific'),
    ]
    nneeded = nall.select([
        'organism', 'organism_scientific', 'enzyme_preferred', 'ncbi', 'NCBI_descriptor'
    ]).unique(['organism', 'enzyme_preferred', 'ncbi'])
    nneeded = compute_fuzz_with_progress(nneeded, NCBI_comparisons).with_columns(
        pl.max_horizontal(
            pl.col(f"similarity_organism_common"),
            pl.col(f"similarity_organism_scientific")
        ).alias('max_organism_similarity'),
    ).select(
        cs.starts_with('similarity_'),
        cs.starts_with('max_'),
        'organism', 'enzyme_preferred', 'ncbi'
    )
    nall = nall.join(nneeded, on=['organism', 'enzyme_preferred', 'ncbi'], how='left', join_nulls=True)

    return uall, pall, nall

    pass




if __name__ == '__main__':

    names = get_wanted_names()

    u, p, n = get_all_cited()
    

    udesc, pdesc, ndesc, platest = get_sequence_descriptions()
    # if pdb_versioned is unavailable, then fill in with the latest version
    platest = platest.rename({'pdb_versioned': 'pdb_latest'}).select('pdb_latest', 'pdb_unversioned')
    p = p.join(platest, left_on='pdb_common', right_on='pdb_unversioned', how='left', coalesce=True).with_columns([
        pl.coalesce([
            pl.col('pdb_versioned'),
            pl.col('pdb_latest')
        ]).alias('pdb_versioned')
    ])

    u, p, n = join_wanted(names, u, p, n)
    u, p, n = add_descriptions(u, p, n, udesc, pdesc, ndesc)

    u, p, n = perform_fuzz(u, p, n)

    # sort 

    # save
    
    print(p)
    u.write_parquet('data/thesaurus/confident/uniprot.parquet')
    p.write_parquet('data/thesaurus/confident/pdb.parquet')
    n.write_parquet('data/thesaurus/confident/ncbi.parquet')
    


    
