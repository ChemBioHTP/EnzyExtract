import polars as pl


def patch_organisms_from_descriptor(df: pl.DataFrame) -> pl.DataFrame:
    """
    Parse organism information from descriptor columns.
    """

    ncbi_taxonomy = pl.read_parquet("data/thesaurus/organism/ncbi_binomial_taxonomy.parquet")

    # encourage fuller matches first
    ncbi_full_names = ncbi_taxonomy.select(
        "name_txt"
    ).unique().sort(
        pl.col("name_txt").str.len_chars(),
        descending=True,
    )["name_txt"]
    ncbi_short_names = ncbi_taxonomy.select(
        "short_genus"
    ).unique().sort(
        pl.col("short_genus").str.len_chars(),
        descending=True,
    )["short_genus"]


    organisms = df.with_columns(
        # exclude "expressed in" -- E. coli is too common
        pl.col("descriptor").str.replace(r"expressed in [^,;]+", "").alias("descriptor_clean")
    ).select(
        pl.col("enzyextract_id"),
        pl.col("descriptor"),
        pl.col("organism"),
        # pl.col("descriptor").str.extract(r"([A-Z]\. [a-z]{4,})").alias("organism.heuristic")
        pl.col("descriptor_clean").str.extract_many(
            # Select the longest match we can
            ncbi_full_names.to_list(),
            leftmost=True,
        ).alias("organism.ncbi_recognized"),
        pl.col("descriptor_clean").str.extract_many(
            ncbi_short_names.to_list(),
            leftmost=True,
        ).alias("organism.ncbi_short_genus")
    )

    # organism_view = organisms.filter(
    #     (pl.col("organism.ncbi_recognized").list.len() > 0)
    #     | (pl.col("organism.ncbi_short_genus").list.len() > 0)
    # )
    fixable_view = organisms.filter(
        ((pl.col("organism.ncbi_recognized").list.len()
         + pl.col("organism.ncbi_short_genus").list.len()) == 1)
        & pl.col("organism").is_null()
    )
    patch = fixable_view.select(
        "enzyextract_id",
        pl.col("organism").alias("organism_old"),  # old value
        pl.coalesce(
            pl.col("organism.ncbi_recognized").list.get(0, null_on_oob=True),
            pl.col("organism.ncbi_short_genus").list.get(0, null_on_oob=True),
        ).alias("organism_patch")  # new value
    )

    return patch
