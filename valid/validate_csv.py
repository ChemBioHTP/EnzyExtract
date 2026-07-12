#!/usr/bin/env python3
"""Lightweight structural validator for EnzyExtract CSV output."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path

NUMBER_RE = re.compile(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?")
KINETIC_UNIT_RE = re.compile(
    r"(?:s|sec|min|h|hr)\s*(?:\^?\s*[-−]?\s*1|⁻¹)|(?:[munµμp]?M|mol(?:ar)?|mol\s*/\s*[lL])",
    re.IGNORECASE,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--required", nargs="*", default=[])
    parser.add_argument("--report", type=Path, default=Path("validation_report.json"))
    args = parser.parse_args()

    errors: list[str] = []
    warnings: list[str] = []
    with args.csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        rows = list(reader)

    missing = [column for column in args.required if column not in columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
    if not rows:
        errors.append("CSV has zero data rows.")

    seen = set()
    duplicates = 0
    for row_number, row in enumerate(rows, start=2):
        signature = tuple((key, (value or "").strip()) for key, value in sorted(row.items()))
        duplicates += signature in seen
        seen.add(signature)
        for field in ("kcat", "Km", "km", "kcat_value", "km_value"):
            value = (row.get(field) or "").strip()
            if value and not NUMBER_RE.search(value):
                warnings.append(f"Row {row_number}, {field}: no numeric token: {value!r}")
            if value and field in {"kcat", "Km", "km"} and not KINETIC_UNIT_RE.search(value):
                warnings.append(f"Row {row_number}, {field}: no recognizable kinetic unit: {value!r}")

    report = {"file": str(args.csv_path), "sha256": sha256(args.csv_path), "row_count": len(rows),
              "column_count": len(columns), "columns": columns, "exact_duplicate_rows": duplicates,
              "errors": errors, "warnings": warnings[:500], "warning_count": len(warnings)}
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
