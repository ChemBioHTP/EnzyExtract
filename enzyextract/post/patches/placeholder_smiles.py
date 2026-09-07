import polars as pl

PLACEHOLDER_SUBSTRATE_BLACKLIST = [
    "fluorogenic substrate",
    "substrate a",
    "chromogenic substrate",
    "fluorescent substrate",
    "chromogenic substrate mimicking chymosin-sensitive site of bovine kappa-casein",
    # these are legit but have no smiles, so do not flag
    "coagulation factor V",
]


def is_placeholder_substrate(col: pl.Expr) -> pl.Expr:
    """
    True if `col` looks like a placeholder substrate name (eg. "compound 4a",
    "substrate II") rather than a real substrate name.
    """
    acceptable = [
        "VX",
        "Calpain Inhibitor II",

        # known chromogenic substrates that are OK
        "Chromogenic substrate S-2366",
        "Chromogenic substrate S-2302",
        "S-2366",
        "S-2288",
        "S-2765",
        "S-2238"
    ]

    return (
        (col.str.contains(
            r"^(([Cc]ompound|[Ss]ubstrate|[Ll]igand|[Ii]nhibitor|[Aa]ctivator|[Aa]ldehyde|[Aa]lcohol|[Ee]ster|[Aa]ldol)\s*)?"
            r"(\d{1,2}[a-z]|[IVX]+|[A-Za-z])$"))  # e.g. 12a, IV, compound A
        | (col.str.contains(r"(?i)\b(compound|substrate|ligand|inhibitor|activator) [A-Za-z0-9]{1,3}\b", literal=False))
        | (col.str.contains(r"(?i)^(inhibitor|activator) [A-Za-z0-9]{1,2}$", literal=False))
        | (col.str.contains(r"^\d{1,2}$"))  # substrate is purely a number
        | (col.str.contains(r"^[A-Za-z]$"))  # substrate is purely a single letter
    ) & ~col.str.contains_any(acceptable, ascii_case_insensitive=True)  # ok: coenzyme, cofactor, cosubstrate


def patch_remove_placeholder_smiles(df: pl.DataFrame) -> pl.DataFrame:
    """
    For rows with `smiles`:

    Null out `smiles`, `cid`, `cid_full`, `brenda_id`, `brenda_id_full` where
    they were assigned to a placeholder substrate name (eg. "compound 4a")
    instead of a real one.

    A placeholder row is spared when its `substrate_full` resolved to a real
    substrate name shared by every occurrence of the placeholder within the
    pmid, in which case these fields genuinely belong to that resolved name.
    """
    placeholder_rows = df.filter(
        pl.col("substrate").str.to_lowercase().is_in(PLACEHOLDER_SUBSTRATE_BLACKLIST)
        | is_placeholder_substrate(pl.col("substrate"))
        | is_placeholder_substrate(pl.col("substrate_full"))
    )

    # rows where the model actually resolved the placeholder to a real substrate
    possible_smiles = placeholder_rows.filter(
        # require that 
        # a) the row has a smiles;
        # b) and the smiles came from substrate_full 
        #   (i.e. cid_full or brenda_id_full have at least 1 hit)
        pl.col("smiles").is_not_null()
        & ((pl.col("cid_full").fill_null([]).list.len() > 0) | pl.col("brenda_id_full").fill_null([]).list.len() > 0)
        & ~is_placeholder_substrate(pl.col("substrate_full"))
    ).select("pmid", "substrate", "substrate_full", "smiles").unique(maintain_order=True, keep="first")
    possible_actual_substrates = possible_smiles.filter(
        # and substrate_full wasn't just the same thing repeated over every placeholder
        # (this also catches cases where, e.g., glutamine = (G, Q))
        ~pl.col("substrate_full").is_duplicated().over("pmid")
    )

    smiles_to_delete = placeholder_rows.filter(pl.col("smiles").is_not_null()).join(
        possible_actual_substrates,
        on=["pmid", "substrate", "substrate_full"],
        how="anti",
    )

    schema = df.schema
    return smiles_to_delete.select(
        "enzyextract_id",
        "substrate",  # diagnostic only; not itself patched
        "substrate_full",  # diagnostic only; not itself patched
        pl.col("smiles").alias("smiles_old"),
        pl.lit(None, dtype=pl.String).alias("smiles_patch"),
        pl.col("cid").alias("cid_old"),
        pl.lit(None, dtype=schema["cid"]).alias("cid_patch"),
        pl.col("cid_full").alias("cid_full_old"),
        pl.lit(None, dtype=schema["cid_full"]).alias("cid_full_patch"),
        pl.col("brenda_id").alias("brenda_id_old"),
        pl.lit(None, dtype=schema["brenda_id"]).alias("brenda_id_patch"),
        pl.col("brenda_id_full").alias("brenda_id_full_old"),
        pl.lit(None, dtype=schema["brenda_id_full"]).alias("brenda_id_full_patch"),
    )
