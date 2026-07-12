#!/usr/bin/env python3
"""Compare canonical extraction records and hallucinations across model runs."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


FIELDS = ("document_id", "parameter_type", "normalized_value", "normalized_unit", "enzyme", "substrate", "mutant")


def clean(value: object) -> str:
    return " ".join(str(value or "").strip().replace("μ", "µ").lower().split())


def record_key(row: dict[str, str]) -> tuple[str, ...]:
    return tuple(clean(row.get(field, "")) for field in FIELDS)


def read_records(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def compare(runs: dict[str, list[dict[str, str]]], negative_documents: set[str]) -> dict:
    keys = {name: {record_key(row) for row in rows} for name, rows in runs.items()}
    result = {"models": {}, "pairwise": {}}
    for name, rows in runs.items():
        negative_rows = [row for row in rows if clean(row.get("document_id")) in negative_documents]
        result["models"][name] = {
            "record_count": len(rows),
            "unique_record_count": len(keys[name]),
            "unsupported_negative_control_records": len(negative_rows),
            "negative_control_false_positive_documents": sorted({clean(row.get("document_id")) for row in negative_rows}),
            "hallucination_gate_pass": not negative_rows,
        }
    names = sorted(runs)
    for index, left in enumerate(names):
        for right in names[index + 1:]:
            intersection = keys[left] & keys[right]
            union = keys[left] | keys[right]
            result["pairwise"][f"{left}__vs__{right}"] = {
                "shared_records": len(intersection),
                "only_left": len(keys[left] - keys[right]),
                "only_right": len(keys[right] - keys[left]),
                "jaccard": len(intersection) / len(union) if union else 1.0,
                "identical": keys[left] == keys[right],
            }
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="append", required=True, metavar="MODEL=CSV")
    parser.add_argument("--negative-document", action="append", default=[])
    parser.add_argument("--negative-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    runs = {}
    for spec in args.run:
        name, separator, filename = spec.partition("=")
        if not separator or not name or not filename:
            parser.error("--run must be MODEL=CSV")
        runs[name] = read_records(Path(filename))
    negatives = {clean(value) for value in args.negative_document}
    if args.negative_manifest:
        with args.negative_manifest.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                if clean(row.get("expected_relevance")) == "negative":
                    negatives.add(clean(row.get("document_id")))
    report = compare(runs, negatives)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    failed = [name for name, metrics in report["models"].items() if not metrics["hallucination_gate_pass"]]
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
