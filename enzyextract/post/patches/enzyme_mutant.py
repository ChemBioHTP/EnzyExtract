import polars as pl

from enzyextract.utils.pl_utils import str_dynamic_replace


def patch_enzyme_names_that_are_mutants(df: pl.DataFrame) -> pl.DataFrame:
    """
    Allow mutants (ie. A123C) to serve as enzyme names.

    The patch *overwrites* enzyme, often with null -- the name was nothing but
    the mutant, which `mutant` already records.

    enzyme_full only ever reads as a parenthetical to enzyme ("enzyme
    (enzyme_full)"), so a row left with a null enzyme and a non-null enzyme_full
    breaks that convention. Where that happens, the full name is promoted into
    enzyme and enzyme_full is cleared.
    """

    enzyme_equals_mutant = df.filter(
        pl.col("mutant").is_not_null()
        # & (pl.col("enzyme") == pl.col("mutant"))
        & (pl.col("enzyme").str.contains(pl.col("mutant"), literal=True))
        & (pl.col("clean_mutant").list.len() > 0)
    )
    enzyme_minus_mutant = enzyme_equals_mutant.with_columns(
        str_dynamic_replace("enzyme", "mutant", "").replace("", None).alias("enzyme_stripped")
    )

    promote = pl.col("enzyme_stripped").is_null() & pl.col("enzyme_full").is_not_null()

    patch = enzyme_minus_mutant.select(
        "enzyextract_id",
        pl.col("enzyme").alias("enzyme_old"),  # old values
        pl.col("enzyme_full").alias("enzyme_full_old"),
        pl.when(promote)  # patched values
        .then(pl.col("enzyme_full"))
        .otherwise(pl.col("enzyme_stripped"))
        .alias("enzyme_patch"),
        pl.when(promote)
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(pl.col("enzyme_full"))
        .alias("enzyme_full_patch"),
    )

    return patch
