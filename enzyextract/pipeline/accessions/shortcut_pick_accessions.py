"""
Functions for picking accession matches (PDB, UniProt, NCBI)
for GPT-extracted enzyme data.

This replaces the inline fuzzy-matching that was previously in
``EnzyExtract.attach_sequences`` (``extractor.py``).
"""

from __future__ import annotations

import polars as pl
from rapidfuzz import fuzz


def _build_accession_records(
    pdb_df: pl.DataFrame,
    uniprot_df: pl.DataFrame,
    ncbi_df: pl.DataFrame,
) -> pl.DataFrame:
    """Build a unified lookup table from the three fetched-sequence DataFrames."""
    records: list[dict] = []

    # PDB
    if pdb_df.height > 0:
        for row in pdb_df.iter_rows(named=True):
            desc = (
                row.get("name")
                or row.get("sys_name")
                or row.get("descriptor")
                or ""
            )
            records.append(
                {
                    "source": "pdb",
                    "accession": str(row.get("pdb", "")),
                    "description": str(desc),
                    "organism": str(row.get("organism") or ""),
                    "sequence": str(row.get("seq_can") or row.get("seq") or ""),
                }
            )

    # UniProt
    if uniprot_df.height > 0:
        for row in uniprot_df.iter_rows(named=True):
            records.append(
                {
                    "source": "uniprot",
                    "accession": str(row.get("uniprot", "")),
                    "description": str(row.get("enzyme_name") or ""),
                    "organism": str(row.get("organism") or ""),
                    "sequence": str(row.get("sequence") or ""),
                }
            )

    # NCBI
    if ncbi_df.height > 0:
        for row in ncbi_df.iter_rows(named=True):
            records.append(
                {
                    "source": "ncbi",
                    "accession": str(row.get("ncbi", "")),
                    "description": str(row.get("descriptor") or ""),
                    "organism": "",
                    "sequence": str(row.get("sequence") or ""),
                }
            )

    return pl.DataFrame(records)


def _empty_schema(name: str) -> pl.DataFrame:
    """Return an empty DataFrame with the schema expected for a given key."""
    schemas: dict[str, pl.Schema] = {
        "uniprot_picked": {
            "pmid": pl.Utf8,
            "enzyme": pl.Utf8,
            "enzyme_full": pl.Utf8,
            "organism": pl.Utf8,
            "uniprot": pl.Utf8,
        },
        "pdb_picked": {
            "pmid": pl.Utf8,
            "enzyme": pl.Utf8,
            "enzyme_full": pl.Utf8,
            "organism": pl.Utf8,
            "pdb": pl.Utf8,
        },
        "ncbi_picked": {
            "pmid": pl.Utf8,
            "enzyme": pl.Utf8,
            "enzyme_full": pl.Utf8,
            "organism": pl.Utf8,
            "ncbi": pl.Utf8,
        },
        "uniprot2seq": {"uniprot": pl.Utf8, "sequence": pl.Utf8},
        "pdb2seq": {"pdb": pl.Utf8, "seq_can": pl.Utf8},
        "ncbi2seq": {"ncbi": pl.Utf8, "sequence": pl.Utf8},
    }
    schema = schemas.get(name)
    if schema is None:
        return pl.DataFrame()
    return pl.DataFrame(schema=schema)


def pick_accessions_by_fuzzy_match(
    gpt_df: pl.DataFrame,
    pdb_df: pl.DataFrame,
    uniprot_df: pl.DataFrame,
    ncbi_df: pl.DataFrame,
    *,
    enzyme_score_threshold: float = 50.0,
    organism_weight: float = 0.3,
) -> dict[str, pl.DataFrame]:
    """
    For each row in *gpt_df*, fuzzy-match the enzyme name and organism
    against the description / organism fields of the fetched accession
    DataFrames (*pdb_df*, *uniprot_df*, *ncbi_df*).

    Parameters
    ----------
    gpt_df : pl.DataFrame
        GPT-extracted enzyme kinetics data.  Must have at least
        ``pmid``, ``enzyme``, ``enzyme_full``, ``organism``.
    pdb_df : pl.DataFrame
        Fetched PDB entries (e.g. from ``pdb_sequences.parquet``).
    uniprot_df : pl.DataFrame
        Fetched UniProt entries (e.g. from ``uniprot_sequences.parquet``).
    ncbi_df : pl.DataFrame
        Fetched NCBI entries (e.g. from ``ncbi_sequences.parquet``).
    enzyme_score_threshold : float
        Minimum ``fuzz.partial_ratio`` score for an accession to be
        considered a match (default 50.0).
    organism_weight : float
        Relative weight given to the organism similarity component
        (default 0.3; the enzyme-name component is ``1 - organism_weight``).

    Returns
    -------
    dict[str, pl.DataFrame]
        A dictionary with the following keys:

        - **uniprot_picked** – mapping ``(pmid, enzyme, enzyme_full, organism) → uniprot``
        - **pdb_picked**     – mapping ``(pmid, enzyme, enzyme_full, organism) → pdb``
        - **ncbi_picked**    – mapping ``(pmid, enzyme, enzyme_full, organism) → ncbi``
        - **uniprot2seq**    – lookup ``uniprot → sequence`` (from *uniprot_df*)
        - **pdb2seq**        – lookup ``pdb → seq_can`` (from *pdb_df*)
        - **ncbi2seq**       – lookup ``ncbi → sequence`` (from *ncbi_df*)
    """
    # -- 1. Build unified accession lookup ----------------------------------
    acc_df = _build_accession_records(pdb_df, uniprot_df, ncbi_df)
    if acc_df.height == 0:
        print("[pick] No accession records found — returning empty picks")
        return {k: _empty_schema(k) for k in
                ("uniprot_picked", "pdb_picked", "ncbi_picked",
                 "uniprot2seq", "pdb2seq", "ncbi2seq")}

    print(f"[pick] Built lookup table with {acc_df.height} accession record(s)")

    # -- 2. Build sequence-lookup tables ------------------------------------
    uniprot2seq = (
        uniprot_df.select(["uniprot", "sequence"])
        .filter(pl.col("sequence").is_not_null())
        .unique("uniprot")
        if uniprot_df.height > 0
        else _empty_schema("uniprot2seq")
    )
    pdb2seq = (
        pdb_df.select(["pdb", "seq_can"])
        .filter(pl.col("seq_can").is_not_null())
        .unique("pdb")
        if pdb_df.height > 0
        else _empty_schema("pdb2seq")
    )
    ncbi2seq = (
        ncbi_df.select(["ncbi", "sequence"])
        .filter(pl.col("sequence").is_not_null())
        .unique("ncbi")
        if ncbi_df.height > 0
        else _empty_schema("ncbi2seq")
    )

    # -- 3. Split lookup by source ------------------------------------------
    src_dfs = {}
    for src_name in ("uniprot", "pdb", "ncbi"):
        src_dfs[src_name] = acc_df.filter(pl.col("source") == src_name)
        if src_dfs[src_name].height == 0:
            print(f"[pick]  No {src_name} records to match against")

    # -- 4. Fuzzy-match each GPT row per source -----------------------------
    enzyme_weight = 1.0 - organism_weight
    match_buckets: dict[str, list[dict]] = {
        "uniprot": [],
        "pdb": [],
        "ncbi": [],
    }
    acc_name_map = {"uniprot": "uniprot", "pdb": "pdb", "ncbi": "ncbi"}

    for row in gpt_df.iter_rows(named=True):
        pmid = str(row.get("pmid") or "")
        enzyme = str(row.get("enzyme") or "")
        organism = str(row.get("organism") or "")
        enzyme_full = str(row.get("enzyme_full") or "")
        query_enzyme = (enzyme_full or enzyme).lower().strip()
        query_organism = organism.lower().strip()

        if not query_enzyme:
            continue

        for src_name, src_df in src_dfs.items():
            best = {"accession": None, "sequence": None,
                    "score_enzyme": 0.0, "score_organism": 0.0,
                    "score_total": 0.0}
            acc_col = acc_name_map[src_name]

            for acc_row in src_df.iter_rows(named=True):
                acc_desc = (acc_row["description"] or "").lower().strip()
                acc_org = (acc_row["organism"] or "").lower().strip()
                if not acc_desc:
                    continue

                score_enzyme = fuzz.partial_ratio(query_enzyme, acc_desc)

                if query_organism and acc_org:
                    score_organism = fuzz.ratio(query_organism, acc_org)
                elif not query_organism and not acc_org:
                    score_organism = 50.0
                else:
                    score_organism = 25.0

                score_total = (
                    score_enzyme * enzyme_weight
                    + score_organism * organism_weight
                )

                if score_total > best["score_total"] and score_enzyme >= enzyme_score_threshold:
                    best.update(
                        accession=acc_row["accession"],
                        sequence=acc_row["sequence"],
                        score_enzyme=score_enzyme,
                        score_organism=score_organism,
                        score_total=score_total,
                    )

            if best["accession"] is not None:
                match_buckets[src_name].append(
                    {
                        "pmid": pmid,
                        "enzyme": enzyme,
                        "enzyme_full": enzyme_full,
                        "organism": organism,
                        acc_col: best["accession"],
                    }
                )

    # -- 5. Build output DataFrames -----------------------------------------
    picked_schemas = {
        "uniprot": {"pmid": pl.Utf8, "enzyme": pl.Utf8, "enzyme_full": pl.Utf8,
                     "organism": pl.Utf8, "uniprot": pl.Utf8},
        "pdb": {"pmid": pl.Utf8, "enzyme": pl.Utf8, "enzyme_full": pl.Utf8,
                "organism": pl.Utf8, "pdb": pl.Utf8},
        "ncbi": {"pmid": pl.Utf8, "enzyme": pl.Utf8, "enzyme_full": pl.Utf8,
                 "organism": pl.Utf8, "ncbi": pl.Utf8},
    }

    result = {}
    for src_name in ("uniprot", "pdb", "ncbi"):
        key = f"{src_name}_picked"
        if match_buckets[src_name]:
            result[key] = pl.DataFrame(match_buckets[src_name],
                                       schema=picked_schemas[src_name])
        else:
            result[key] = _empty_schema(key)

    result["uniprot2seq"] = uniprot2seq
    result["pdb2seq"] = pdb2seq
    result["ncbi2seq"] = ncbi2seq

    total = sum(len(v) for k, v in result.items() if k.endswith("_picked"))
    print(f"[pick] Matched {total} accession(s) across all sources")
    return result
