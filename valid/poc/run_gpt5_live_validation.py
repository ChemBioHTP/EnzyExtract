#!/usr/bin/env python3
"""Run live-only GPT-5 family synthetic validation for EnzyExtract."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SUITE = ROOT.parent / "EnzyExtract_Validation_Suite_v2"
PYTHON = Path("/home/ranx/.conda/envs/py3/bin/python")
COLUMN_MAP = SUITE / "templates/prediction_column_map.enzyextract_wide.json"
SCORER = SUITE / "scripts/score_extraction.py"
GATES = SUITE / "scripts/evaluate_gates.py"
STRICT_CRITERIA = Path(__file__).with_name("strict_hallucination_criteria.json")


def slug_model(model: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "-", model.removeprefix("openai/")).strip("-")


def run_command(args: list[str], *, cwd: Path, env: dict[str, str], ok_returncodes: set[int] | None = None) -> None:
    print("+", " ".join(args), flush=True)
    completed = subprocess.run(args, cwd=cwd, env=env, check=False)
    expected = {0} if ok_returncodes is None else ok_returncodes
    if completed.returncode not in expected:
        raise subprocess.CalledProcessError(completed.returncode, args)


def latest_completion(run_dir: Path) -> Path:
    candidates = sorted((run_dir / "completions").glob("*.jsonl"))
    if not candidates:
        raise RuntimeError(f"No completion JSONL was written under {run_dir / 'completions'}")
    return candidates[-1]


def assert_live_completion(path: Path, expected_count: int) -> None:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    if len(rows) != expected_count:
        raise RuntimeError(f"{path} contains {len(rows)} completions; expected {expected_count}")
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
    if failures:
        raise RuntimeError(f"Completion JSONL contains non-live or failed responses: {failures[:3]}")


def count_manifest_documents(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def summarize_metrics(model: str, metrics_path: Path, gate_path: Path) -> dict[str, object]:
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    gates = json.loads(gate_path.read_text(encoding="utf-8"))
    record = metrics["record_level"]
    safety = metrics["safety"]
    document = metrics["document_level"]
    return {
        "model": model,
        "predicted_records": metrics["metadata"]["predicted_records"],
        "strict_precision": record["strict_precision"],
        "strict_recall": record["strict_recall"],
        "strict_f1": record["strict_f1"],
        "negative_control_false_positive_documents": document["negative_control_false_positive_documents"],
        "unsupported_record_count": safety["unsupported_record_count"],
        "micro_milli_unflagged_error_count": safety["micro_milli_unflagged_error_count"],
        "catastrophic_numeric_error_count_1000x": safety["catastrophic_numeric_error_count_1000x"],
        "strict_hallucination_gates_pass": gates["overall_pass"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus-dir", type=Path, required=True)
    parser.add_argument("--gold", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, default=Path("validation_runs/gpt5-poc"))
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument("--bootstrap", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=20260712)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--reocr-model-path", default="data/models/resnet18-remicro-iter3.pth")
    args = parser.parse_args()

    if not os.environ.get("OPENAI_API_KEY") and not args.env_file.exists():
        raise SystemExit("OPENAI_API_KEY or --env-file is required; live validation cannot be mocked.")
    if not SCORER.exists():
        raise SystemExit(f"Missing scorer: {SCORER}")

    env = os.environ.copy()
    expected_count = count_manifest_documents(args.manifest)
    summaries = []
    for model in args.model:
        slug = slug_model(model)
        namespace = f"synthetic-{slug}"
        run_dir = args.run_root / slug
        run_dir.mkdir(parents=True, exist_ok=True)
        enzy_root = run_dir / ".enzy"
        pred_csv = run_dir / "kinetics.csv"
        scoring_dir = run_dir / "scoring"

        run_command([
            str(PYTHON), "-m", "enzyextract",
            "--enzy-root", str(enzy_root),
            "--llm-name", model,
            "--env-file", str(args.env_file),
            "submit",
            "--pdf-root", str(args.corpus_dir),
            "--namespace", namespace,
            "--mode", "interactive",
            "--reocr-model-path", args.reocr_model_path,
        ], cwd=ROOT, env=env)

        assert_live_completion(latest_completion(enzy_root), expected_count)

        run_command([
            str(PYTHON), "-m", "enzyextract",
            "--enzy-root", str(enzy_root),
            "--env-file", str(args.env_file),
            "download",
            "--namespace", namespace,
            "--output-csv", str(pred_csv),
        ], cwd=ROOT, env=env)

        run_command([
            str(PYTHON), str(SCORER),
            "--gold", str(args.gold),
            "--pred", str(pred_csv),
            "--column-map", str(COLUMN_MAP),
            "--document-manifest", str(args.manifest),
            "--output-dir", str(scoring_dir),
            "--bootstrap", str(args.bootstrap),
            "--seed", str(args.seed),
        ], cwd=ROOT, env=env)

        run_command([
            str(PYTHON), str(GATES),
            "--metrics", str(scoring_dir / "metrics.json"),
            "--criteria", str(STRICT_CRITERIA),
            "--output", str(scoring_dir / "strict_hallucination_gate_report.json"),
        ], cwd=ROOT, env=env, ok_returncodes={0, 1})

        summaries.append(summarize_metrics(
            model,
            scoring_dir / "metrics.json",
            scoring_dir / "strict_hallucination_gate_report.json",
        ))

    summary_path = args.run_root / "gpt5_model_metrics_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps({"models": summaries}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"models": summaries}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
