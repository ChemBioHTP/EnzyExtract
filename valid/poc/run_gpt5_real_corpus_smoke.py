#!/usr/bin/env python3
"""Run live GPT-5 family smoke checks on the 16-article real PDF/XML corpus."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[2]
PYTHON = Path("/home/ranx/.conda/envs/py3/bin/python")


def slug_model(model: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", model.removeprefix("openai/")).strip("-")


def run_command(args: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    print("+", " ".join(args), flush=True)
    subprocess.run(args, cwd=cwd, env=env, check=True)


def manifest_rows(manifest: Path) -> list[dict[str, str]]:
    with manifest.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def count_completion_rows(path: Path) -> tuple[int, int, list[dict[str, object]]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    failures = []
    for row in rows:
        response = row.get("response") or {}
        body = response.get("body") or {}
        if response.get("status_code") != 200 or not body.get("id") or not body.get("choices"):
            failures.append({
                "custom_id": row.get("custom_id"),
                "status_code": response.get("status_code"),
                "error": response.get("error"),
            })
    return len(rows), len(rows) - len(failures), failures


def latest_completion(enzy_root: Path) -> Path:
    candidates = sorted((enzy_root / "completions").glob("*.jsonl"))
    if not candidates:
        raise RuntimeError(f"No live completion JSONL was written under {enzy_root / 'completions'}")
    return candidates[-1]


def dataframe_height(path: Path) -> int:
    if not path.exists():
        return 0
    if path.suffix == ".csv":
        return pl.read_csv(path).height
    if path.suffix == ".parquet":
        return pl.read_parquet(path).height
    raise ValueError(f"Unsupported tabular file: {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--article-root", type=Path, default=Path("valid/corpus/articles"))
    parser.add_argument("--run-root", type=Path, default=Path("validation_runs/gpt5-real-corpus-smoke"))
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--reocr-model-path", default="data/models/resnet18-remicro-iter3.pth")
    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY") and not args.env_file.exists():
        raise SystemExit("OPENAI_API_KEY or --env-file is required; live real-corpus smoke cannot be mocked.")

    manifest = args.article_root / "corpus_manifest.csv"
    rows = manifest_rows(manifest)
    pdf_rows = [row for row in rows if row["format"].startswith("pdf")]
    xml_rows = [row for row in rows if row["format"] == "xml"]
    if len(rows) != 16 or len(pdf_rows) != 11 or len(xml_rows) != 5:
        raise SystemExit(f"Expected 16 corpus rows (11 PDF, 5 XML); found {len(rows)} rows.")

    pdf_root = args.article_root / "pdf"
    xml_root = args.article_root / "xml"
    env = os.environ.copy()
    summaries = []

    for model in args.model:
        slug = slug_model(model)
        namespace = f"real16-{slug}"
        run_dir = args.run_root / slug
        enzy_root = run_dir / ".enzy"
        pred_csv = run_dir / "kinetics.csv"
        xml_enzy_root = run_dir / ".enzy-xml"

        run_command([
            str(PYTHON), "-m", "enzyextract",
            "--enzy-root", str(enzy_root),
            "--llm-name", model,
            "--env-file", str(args.env_file),
            "submit",
            "--pdf-root", str(pdf_root),
            "--namespace", namespace,
            "--mode", "interactive",
            "--reocr-model-path", args.reocr_model_path,
        ], cwd=ROOT, env=env)

        completion = latest_completion(enzy_root)
        completion_rows, live_successes, failures = count_completion_rows(completion)
        if completion_rows != len(pdf_rows):
            raise RuntimeError(f"{completion} has {completion_rows} rows; expected {len(pdf_rows)} PDF completions")
        if failures:
            raise RuntimeError(f"{completion} has failed/non-live rows: {failures[:3]}")

        run_command([
            str(PYTHON), "-m", "enzyextract",
            "--enzy-root", str(enzy_root),
            "--env-file", str(args.env_file),
            "download",
            "--namespace", namespace,
            "--output-csv", str(pred_csv),
        ], cwd=ROOT, env=env)

        run_command([
            str(PYTHON), "-m", "enzyextract",
            "--enzy-root", str(xml_enzy_root),
            "--env-file", str(args.env_file),
            "xml",
            "--xml-root", str(xml_root),
        ], cwd=ROOT, env=env)

        xml_parquet = xml_enzy_root / "scans/xml/xml.parquet"
        xml_processed = dataframe_height(xml_parquet)
        summaries.append({
            "model": model,
            "corpus_documents": len(rows),
            "pdf_documents": len(pdf_rows),
            "xml_documents": len(xml_rows),
            "pdf_live_completion_rows": completion_rows,
            "pdf_live_completion_successes": live_successes,
            "xml_processed_rows": xml_processed,
            "extracted_record_rows": dataframe_height(pred_csv),
            "terminal_status_rate": (live_successes + min(xml_processed, len(xml_rows))) / len(rows),
            "precision_recall_f1": "not_computed_no_adjudicated_gold_for_16_real_corpus",
        })

    summary_path = args.run_root / "real16_smoke_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps({"models": summaries}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"models": summaries}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
