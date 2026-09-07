from pathlib import Path

import polars as pl

from enzyextract.post.patches.enzyme_mutant import patch_enzyme_names_that_are_mutants
from enzyextract.post.patches.improper_substrate_full import patch_null_improper_substrate_full
from enzyextract.post.patches.organism import patch_organisms_from_descriptor
from enzyextract.post.patches.placeholder_smiles import patch_remove_placeholder_smiles

PATCH_DIR = "data/migrate/patches"
ID_COL = "enzyextract_id"


def patched_columns(patch: pl.DataFrame) -> list:
    """
    Infer which columns a patch frame targets, from its `<column>_patch` columns.

    A patch usually rewrites a single column, but it may rewrite several at once
    when they only make sense together (ie. enzyme and enzyme_full).
    """
    names = [c[: -len("_patch")] for c in patch.columns if c.endswith("_patch")]
    if not names:
        raise ValueError("Expected at least one `*_patch` column, found none")
    return names


def patched_column(patch: pl.DataFrame) -> str:
    """
    Infer the single column a patch frame targets. See `patched_columns`.
    """
    names = patched_columns(patch)
    if len(names) != 1:
        raise ValueError(f"Expected exactly one `*_patch` column, found: {names}")
    return names[0]


def apply_patch(
    df: pl.DataFrame,
    patch: pl.DataFrame,
    column=None,
    on: str = ID_COL,
    on_conflict: str = "warn",
) -> pl.DataFrame:
    """
    Apply a patch frame directly to `df`, instead of recomputing the migration.

    A patch frame holds `on`, `<column>_old` and `<column>_patch`: the value the
    row is expected to hold, and the value it should be replaced with. `column`
    may be a single name or a list of names, and defaults to whatever
    `<column>_patch` columns the frame carries.

    on_conflict controls rows whose current value doesn't match `<column>_old`:
        "warn"   -- leave those rows untouched, print how many were skipped
        "error"  -- raise
        "ignore" -- patch them anyway

    A multi-column patch applies atomically: if any of its columns conflict, the
    whole row is left alone, so the columns never disagree with each other.
    """
    if column is None:
        columns = patched_columns(patch)
    elif isinstance(column, str):
        columns = [column]
    else:
        columns = list(column)

    old_cols = [f"{c}_old" for c in columns]
    new_cols = [f"{c}_patch" for c in columns]

    missing = ({on} | set(old_cols) | set(new_cols)) - set(patch.columns)
    if missing:
        raise ValueError(f"Patch is missing columns: {sorted(missing)}")
    absent = [c for c in columns if c not in df.columns]
    if absent:
        raise ValueError(f"Column(s) {absent} not in df")

    joined = df.join(patch.select(on, *old_cols, *new_cols), on=on, how="left")

    # a patch row applies when the id matched and every current value is what the
    # patch was computed against (null-safe, since many patches fill in nulls)
    matched = pl.col(on).is_in(patch[on].implode())
    unchanged = pl.all_horizontal(
        pl.col(c).eq_missing(pl.col(old)) for c, old in zip(columns, old_cols)
    )

    if on_conflict == "ignore":
        applies = matched
    else:
        applies = matched & unchanged
        n_conflict = joined.filter(matched & ~unchanged).height
        if n_conflict:
            msg = f"{n_conflict} row(s) no longer match {old_cols}"
            if on_conflict == "error":
                raise ValueError(msg)
            print(f"[patch] {msg}; leaving them untouched")

    return joined.with_columns(
        pl.when(applies).then(pl.col(new)).otherwise(pl.col(c)).alias(c)
        for c, new in zip(columns, new_cols)
    ).drop(*old_cols, *new_cols)


def load_patch(name: str, patch_dir: str = PATCH_DIR) -> pl.DataFrame:
    """
    Read a patch by name, e.g. "v1.1_organism".
    """
    if not name.endswith(".parquet"):
        name = f"{name}.parquet"
    return pl.read_parquet(f"{patch_dir}/{name}")


# Every migration is a *patch generator*: df -> patch frame. Nothing mutates a
# frame directly; application always goes through `apply_patch`, so a migration
# behaves identically whether it was just computed or replayed from disk.
MIGRATIONS = {
    "v1.1_enzyme_mutant": patch_enzyme_names_that_are_mutants,
    "v1.1_organism": patch_organisms_from_descriptor,
    "v1.1_improper_substrate_full": patch_null_improper_substrate_full,
    "v1.1_placeholder_smiles": patch_remove_placeholder_smiles,
}

V1_1_PATCHES = [
    "v1.1_enzyme_mutant",
    "v1.1_organism",
    "v1.1_improper_substrate_full",
    "v1.1_placeholder_smiles",
]


def run_migrations(
    df: pl.DataFrame,
    names: list,
    reuse: bool = True,
    patch_dir: str = PATCH_DIR,
    on_conflict: str = "warn",
) -> pl.DataFrame:
    """
    Run migrations in order as generate-patch -> apply-patch.

    Each step reuses its saved patch when one exists (skipping the expensive
    derivation), and otherwise generates and saves it. Either way the patch is
    then applied, so the next step's generator sees the patched frame -- the
    same frame it would see on a full regeneration.

    Pass reuse=False to force every patch to be rebuilt from scratch.
    """
    Path(patch_dir).mkdir(parents=True, exist_ok=True)

    for name in names:
        path = Path(patch_dir) / f"{name}.parquet"
        if reuse and path.exists():
            patch = pl.read_parquet(path)
            action = "reused"
        else:
            patch = MIGRATIONS[name](df)
            patch.write_parquet(path)
            action = "built"
        df = apply_patch(df, patch, on_conflict=on_conflict)
        print(f"[patch] {action} {name} ({patch.height} rows)")
    return df


def migrate_df(df: pl.DataFrame, reuse: bool = True) -> pl.DataFrame:
    """
    Bring TheData up to v1.1, reusing any patches already on disk.
    """
    # v1.1
    df = df.with_row_index("enzyextract_id")
    print("Applying v1.1 migrations...")
    _starting_organisms = df["organism"].drop_nulls().len()

    df = run_migrations(df, V1_1_PATCHES, reuse=reuse)

    print(f"[v1.1] Went from {_starting_organisms} to {df['organism'].drop_nulls().len()} organisms")
    df.write_parquet("data/export/TheData_unpruned_v1.1_alpha.parquet")
    print("Wrote data/export/TheData_unpruned_v1.1_alpha.parquet")
    return df


def create_thedata_kcat():
    thedata = pl.read_parquet("data/export/TheData_unpruned_v1.1_alpha.parquet").filter(
        pl.col("kcat").is_not_null()
        & pl.col("flag.repetitive").is_null()
        & pl.col("flag.hallucination").is_null()
        & pl.col("flag.scientific").is_null()
    )
    thedata.write_parquet("EnzyExtractDB/EnzyExtractDB_176463_v1.1_alpha.parquet")


if __name__ == "__main__":
    df = pl.read_parquet("data/export/TheData_unpruned.parquet")

    migrate_df(df)

    create_thedata_kcat()