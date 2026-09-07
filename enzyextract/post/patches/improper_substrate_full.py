import polars as pl

SUBSTRATE_SYNONYMS_PATH = "data/recontext/from_yaml/enzyextract_substrate_synonyms.parquet"
CID2SMILES_PATH = "data/thesaurus/substrate/cid2smiles.parquet"
BRENDA_INCHI_ALL_PATH = "data/brenda/brenda_inchi_all.parquet"
BRENDA_INCHI_SMILES_PATH = "data/brenda/brenda_inchi_smiles.parquet"

# see experiments/00_substrate_improper.py for how this whitelist was derived:
# these are the prefixes/suffixes around `substrate` (once stripped out of
# `substrate_full`) that are benign, ie. `substrate_full` still genuinely
# describes `substrate` rather than a different, related compound.
WHITELIST_PRECEDING = [
    "D-", "L-", "d-", "l-", "(R)-", "(S)-",
    "(R)-(-)-", "(S)-(+-)", "d-(+)-", "(+)-",
    "secondary alcohol ",
    "activated ",
    "peptide ",
    "organic ",
    "proteinase domain of ",
    "spin-labeled ",
    "exogenous ",
    "bound ",
]
BENIGN_PREFIXES = [
    "substrate ",
    "analog ",
    "analogue ",
    "compound ",
    "derivative ",
    "peptide ",
]
BENIGN_SUFFIXES = [
    " tripeptide",
    " peptide",
]


def find_improper_substrate_full_rows(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rows where `substrate` was only ever attached to `substrate_full` because
    it happens to be a substring of it, but isn't actually a synonym of it
    (eg. "glucose" is a substring of "n-acetylglucosamine", but the two are
    different compounds).

    See experiments/00_substrate_improper.py for how this heuristic was
    derived.
    """
    is_substring = df.filter(
        pl.col("substrate_full").str.contains(pl.col("substrate"), literal=True)
    )

    substrate_context = pl.read_parquet(SUBSTRATE_SYNONYMS_PATH).rename({
        "fullname": "substrate_full",
        "synonyms": "substrate",
    })

    improper = is_substring.join(
        substrate_context.select("substrate", "substrate_full"),
        on=["substrate", "substrate_full"],
        how="anti",
        nulls_equal=True,
    )

    # spared when cid and cid_full already overlap: the substring assumption
    # happens to be right, so substrate_full isn't actually improper here
    overlap = improper.filter(
        pl.col("cid").list.set_intersection(pl.col("cid_full")).list.len() > 0
    )

    preceding = pl.col("substrate_full").str.slice(
        0, pl.col("substrate_full").str.find(pl.col("substrate"), literal=True)
    ).replace("", None)
    following = pl.col("substrate_full").str.slice(
        pl.col("substrate_full").str.find(pl.col("substrate"), literal=True) + pl.col("substrate").str.len_chars(),
        None,
    ).replace("", None)

    improper = improper.with_columns(
        preceding.alias("_substrate_preceding"),
        following.alias("_substrate_following"),
    )

    acceptable = improper.filter(
        (pl.col("_substrate_following").is_null() & (
            pl.col("_substrate_preceding").is_in(WHITELIST_PRECEDING)
            | pl.col("_substrate_preceding").str.contains_any(BENIGN_PREFIXES, ascii_case_insensitive=True)
            | pl.col("_substrate_preceding").str.contains(r"^\[.*(2H|3H|13C|14C|15N|32P)\]$")
            | pl.col("_substrate_preceding").str.contains(r"^(2H|3H|13C|14C|15N|32P)-$")
        ))
        | (pl.col("_substrate_preceding").is_null()
           & pl.col("_substrate_following").str.contains_any(BENIGN_SUFFIXES, ascii_case_insensitive=True))
    )
    acceptable = pl.concat([acceptable.drop("_substrate_preceding", "_substrate_following"), overlap], how="diagonal_relaxed")

    unacceptable = improper.join(
        acceptable.select("enzyextract_id"),
        on="enzyextract_id",
        how="anti",
    )
    return unacceptable.drop("_substrate_preceding", "_substrate_following")


def _first_non_null_smiles(df: pl.DataFrame, list_col: str, mapping: pl.DataFrame, key: str) -> pl.Series:
    """
    For each row of `df`, the first `smiles` in `mapping` reached by walking
    `df[list_col]` (a list column) in order, or null if none of its entries
    are in `mapping`.
    """
    exploded = (
        df.select(list_col)
        .with_row_index("_row")
        .explode(list_col)
        .with_row_index("_pos")
    )
    joined = exploded.join(
        mapping.rename({key: list_col, "smiles": "_hit"}).select(list_col, "_hit"),
        on=list_col,
        how="left",
    )
    first_hit = (
        joined.filter(pl.col("_hit").is_not_null())
        .sort("_pos")
        .group_by("_row", maintain_order=False)
        .agg(pl.col("_hit").first())
    )
    return (
        df.select(list_col)
        .with_row_index("_row")
        .join(first_hit, on="_row", how="left")
        .sort("_row")["_hit"]
    )


def _load_cid2smiles() -> pl.DataFrame:
    return (
        pl.read_parquet(CID2SMILES_PATH)
        .select("CID", "SMILES")
        .rename({"CID": "cid", "SMILES": "smiles"})
        .drop_nulls()
        .unique(subset="cid", keep="first")
    )


def _load_brenda_id2smiles() -> pl.DataFrame:
    # brenda_inchi_smiles already maps brenda_id -> smiles directly; enrich it
    # with brenda_inchi_all's much larger brenda_id -> inchi table, joined
    # back on inchi, to cover brenda_ids the direct table is missing
    direct = (
        pl.read_parquet(BRENDA_INCHI_SMILES_PATH)
        .select("brenda_id", "smiles")
        .drop_nulls()
    )
    inchi2smiles = direct.join(
        pl.read_parquet(BRENDA_INCHI_ALL_PATH).select("brenda_id", "inchi"),
        on="brenda_id",
    ).select("inchi", "smiles").unique(subset="inchi", keep="first")
    via_inchi = (
        pl.read_parquet(BRENDA_INCHI_ALL_PATH)
        .select("brenda_id", "inchi")
        .join(inchi2smiles, on="inchi", how="inner")
        .select("brenda_id", "smiles")
    )
    return pl.concat([direct, via_inchi]).unique(subset="brenda_id", keep="first")


def patch_null_improper_substrate_full(df: pl.DataFrame) -> pl.DataFrame:
    """
    For rows where `substrate_full` doesn't actually describe `substrate` (see
    `find_improper_substrate_full_rows`): trust `substrate` instead, and null
    out `substrate_full`, `cid_full`, and `brenda_id_full`.

    `smiles` is normally taken from `substrate_full` first, then `substrate`.
    Since `substrate_full` is being discarded here, `smiles` is recomputed
    from `substrate`'s own `cid`/`brenda_id` (first non-null match, `cid`
    taking priority over `brenda_id`).
    """
    improper = find_improper_substrate_full_rows(df)

    cid2smiles = _load_cid2smiles()
    brenda_id2smiles = _load_brenda_id2smiles()

    smiles_via_cid = _first_non_null_smiles(improper, "cid", cid2smiles, "cid")
    smiles_via_brenda = _first_non_null_smiles(improper, "brenda_id", brenda_id2smiles, "brenda_id")
    new_smiles = smiles_via_cid.fill_null(smiles_via_brenda)

    schema = df.schema
    return improper.select(
        "enzyextract_id",
        "substrate",  # diagnostic only; not itself patched
        pl.col("substrate_full").alias("substrate_full_old"),
        pl.lit(None, dtype=schema["substrate_full"]).alias("substrate_full_patch"),
        pl.col("cid_full").alias("cid_full_old"),
        pl.lit(None, dtype=schema["cid_full"]).alias("cid_full_patch"),
        pl.col("brenda_id_full").alias("brenda_id_full_old"),
        pl.lit(None, dtype=schema["brenda_id_full"]).alias("brenda_id_full_patch"),
        pl.col("smiles").alias("smiles_old"),
        new_smiles.alias("smiles_patch"),
    )
