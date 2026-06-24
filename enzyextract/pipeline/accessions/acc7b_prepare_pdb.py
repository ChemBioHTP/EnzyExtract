
from pathlib import Path

import polars as pl
from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.thesaurus.ascii_patterns import pl_to_ascii
from enzyextract.thesaurus.organism_patterns import pl_fix_organism
from enzyextract.thesaurus.fuzz_utils import compute_fuzz_with_progress

@resolve
def script_prepare_possible_pdb(
    df = REQUIRE("data/export/TheData_kcat.parquet"),
    sequence_scans_df = REQUIRE("data/enzymes/sequence_scans/latest_sequence_scans.parquet"),
    backcited = REQUIRE("data/thesaurus/enzymes/backcited.parquet"),

    pdb_all = REQUIRE("data/enzymes/accessions/final/pdb.parquet"),
    organism_df = REQUIRE("data/thesaurus/organism/uniprot_organism.parquet"),
):
    """
    Prepare a dataframe of PDB IDs, with their associated enzyme names and organisms.
    The most relevant ID is selected based on heuristics and LLM-based fuzzy matching.
    """
    kinetic_papers = df.filter(
        pl.col("kcat").is_not_null()
    ).select("pmid", "canonical").unique("canonical", keep="first", maintain_order=True)

    infos = df.select([
        "pmid",
        "enzyme",
        "enzyme_full",
        "organism",
        "canonical",
    ]).unique()

    infos = infos.filter(
        pl.col("enzyme").is_not_null()
        | pl.col("enzyme_full").is_not_null()
    ).with_row_index("index")


    # Get PDBs: the latest version of each pdb
    pdb_all = pdb_all.filter(
        ~pl.col("seq_can").str.contains("X")
    )
    pdb_unversioned = pdb_all.sort("pdb_version", descending=True).unique("pdb_unversioned", keep="first")


    # Get forward cited Accessions per each PMID
    sequence_scans_df = sequence_scans_df.select("pmid", "pdb", "has_pdb").filter(
        pl.col("has_pdb")
    )
    # complete the map from pmid to accessions
    sequence_scans_df = sequence_scans_df.group_by("pmid").agg(
        pl.col("pdb").drop_nulls().flatten().unique(),
    ).select([
        "pmid", "pdb",
    ]) # should be only 1 pmid

    # NOTE: some PMIDs are lost here, but that's okay
    sequence_scans_df = sequence_scans_df.join(kinetic_papers, left_on="pmid", right_on="pmid", how="inner")

    # Get backward cited accessions per each PMID (when a PDB itself cites a PMID)

    # combine them
    policy = "concat_backcited"
    # policy = "delete_backcited"

    if policy == "concat_backcited":
        doc2accessions = sequence_scans_df.select("pmid", "canonical", "pdb").join(
            backcited.select("canonical", "pdb"), left_on="canonical", right_on="canonical", how="full", suffix="_back", coalesce=True)
        doc2accessions = doc2accessions.with_columns([
            pl.col("pdb").fill_null([]).list.concat(pl.col("pdb_back").fill_null([])).alias("pdb"),
        ])

        doc2preferred = doc2accessions.select([
            pl.coalesce(pl.col("pdb_back"), pl.col("pdb")).alias("pdb"),
            pl.col("canonical"),
            pl.col("pmid")
        ])
    elif policy == "delete_backcited":
        doc2accessions = sequence_scans_df.select("pmid", "canonical", "pdb").join(
            backcited.select("canonical", "pdb"), left_on="canonical", right_on="canonical", how="anti")
        doc2preferred = doc2accessions.select([
            pl.col("pdb"),
            pl.col("canonical"),
            pl.col("pmid")
        ])

    # join to accessions
    infos = infos.join(doc2preferred, left_on="canonical", right_on="canonical", how="inner")

    # now do a fancy match
    infos = infos.explode("pdb")

    infos = infos.filter(
        # remove those that have both lower and uppercase, for instance mM
        ~(pl.col("pdb").str.contains(r"[a-z]") & pl.col("pdb").str.contains(r"[A-Z]"))
        # these are also unlikely candidates: min, 111s, mg
    )
    infos_versioned = infos.filter(pl.col("pdb").str.contains("_"))
    infos_unversioned = infos.filter(~pl.col("pdb").str.contains("_")).rename({"pdb": "pdb_unversioned"})

    infos_versioned = infos_versioned.join(pdb_all, left_on="pdb", right_on="pdb", how="inner")


    # use_lowercase = "lower_only" 
    # use_lowercase = "upper_only" 
    use_lowercase = "both" 

    if use_lowercase == "both": # need to convert lower to upper
        infos_unversioned = infos_unversioned.with_columns([
            pl.col("pdb_unversioned").str.to_uppercase().alias("pdb_unversioned")
        ])
    elif use_lowercase == "upper_only":
        pass
        # NOTE: the join implicitly filters out lowercase PDBs
    elif use_lowercase == "lower_only":
        # filter
        infos_unversioned = infos_unversioned.filter(
            ~pl.col("pdb_unversioned").str.contains(r"[A-Z]")
        ).with_columns([
            pl.col("pdb_unversioned").str.to_uppercase().alias("pdb_unversioned")
        ])
    infos_unversioned = infos_unversioned.join(pdb_unversioned, on="pdb_unversioned", how="inner")


    infos = pl.concat([infos_versioned, infos_unversioned], how="diagonal")
    infos = infos.unique(["index", "pdb"]) # if pdb versioned and pdb unversioned coincide. 46603 to 

    # this is kind of crazy to have 10+ pdbs for one enzyme. let"s just filter those out for now. 

    info_view = infos.select("index", "pmid", "canonical", "enzyme", "enzyme_full", "organism", "pdb", 
                        "descriptor", "name", "sys_name", "organism_right", "info")

    ### Load organism thesaurus
    organism_df = organism_df.drop_nulls()
    # NOTE: right now, it selects the lexicographically first organism. It might be better to select the most common one.
    organism_df = (
        organism_df
        .with_columns([
            pl.col("organism_common").str.to_lowercase().alias("organism_common"),
        ]).sort("frequency", descending=True)
        .unique("organism_common", keep="first")
        .select(["organism_common", "organism"])
        .rename({"organism": "organism_uniprot"})
    )

    ### Fix organisms
    # 1. use uniprot organism thesaurus. write organism_uniprot
    info_view = info_view.with_columns([
        pl.col("organism").str.to_lowercase().alias("organism_lower"),
    ])
    info_view = info_view.join(organism_df, left_on="organism_lower", right_on="organism_common", how="left", validate="m:1")

    # 2. use manually written corrections on dictionary.
    info_view = info_view.with_columns([
        pl_to_ascii(
            pl.coalesce(pl.col("enzyme_full"), pl.col("enzyme"))
        ).alias("enzyme_preferred"),
        pl_fix_organism(pl.col("organism")).alias("organism_fixed"),
        pl_fix_organism(pl.col("organism_right")).alias("organism_right"), # just in case
    ])

    # Define the comparisons to calculate fuzz similarities
    comparisons = [
        ("organism_fixed", "organism_right", False, "similarity_organism_simple"),
        ("organism_uniprot", "organism_right", False, "similarity_organism_common"),
        ("enzyme_preferred", "descriptor", False, None),
        ("enzyme_preferred", "name", False, None),
        ("enzyme_preferred", "info", False, None)
    ]

    # Compute the similarities with progress tracking
    info_view = compute_fuzz_with_progress(info_view, comparisons).with_columns(
        pl.max_horizontal(
            pl.col("similarity_enzyme_preferred_vs_descriptor"),
            pl.col("similarity_enzyme_preferred_vs_name"),
            pl.col("similarity_enzyme_preferred_vs_info")
        ).alias("max_enzyme_similarity"),
        pl.max_horizontal(
            pl.col("similarity_organism_simple"),
            pl.col("similarity_organism_common")
        ).alias("max_organism_similarity")
    ).select([
        "index", "pmid", "canonical", 
        "enzyme", "enzyme_full", "enzyme_preferred", 
        "organism", "organism_fixed", "organism_uniprot", 
        "pdb", "descriptor", "name", "sys_name", "organism_right", "info", 
        "max_organism_similarity", "max_enzyme_similarity",
    ]).sort("index")


    if False:
        # organism similarity 95 to 90: 9342 --> 9486
        # enzyme similarity 90 to 85: 9486 --> 10343
        perfect_pdb = info_view.filter((pl.col("max_enzyme_similarity") >= 85) & (pl.col("max_organism_similarity") >= 90))
        imperfect_pdb = info_view.filter((pl.col("max_enzyme_similarity") < 90) | (pl.col("max_organism_similarity") < 95))
        imperfect_pdb = imperfect_pdb.join(perfect_pdb, on="index", how="anti") # perfect pdb no longer needs to be matched.
        pass # 46843 to 29401

        pdb_no_organism = info_view.filter((pl.col("max_enzyme_similarity") > 99) 
                                        & (pl.col("max_organism_similarity").is_null())
                                        & ~pl.col("index").is_in(perfect_pdb["index"]))
        # print("Similars at data/thesaurus/enzymes/pdb_similar.parquet")
        # pdb_no_organism.write_parquet(f"data/thesaurus/enzymes/pdb_similar_no_organism.parquet")

        perfect_pdb = perfect_pdb.with_columns([
            (pl.col("max_enzyme_similarity") + pl.col("max_organism_similarity")).alias("total_similarity")
        ])
        # perfect_pdb.write_parquet(f"data/thesaurus/enzymes/pdb_similar.parquet")

    # write
    # data/ingest/pick/pick-pdb-dev3.parquet
    write_to = Path("data/enzymes/pick/pick-pdb-v1.parquet")
    write_to.parent.mkdir(parents=True, exist_ok=True)
    info_view.write_parquet(write_to)
    print("Ingest at", write_to)
    return info_view



if __name__ == "__main__":
    script_prepare_possible_pdb()
