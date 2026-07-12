#!/usr/bin/env bash
set -euo pipefail

PYTHON="${PYTHON:-python}"
SUITE_DIR="${VALIDATION_SUITE_DIR:-$(cd "$(dirname "$0")/../.." && pwd)/EnzyExtract_Validation_Suite_v2}"
OUT_DIR="${1:-validation_offline}"

"$PYTHON" -m pytest -q tests/valid
"$PYTHON" -m pytest -q "$SUITE_DIR/tests"
"$PYTHON" "$SUITE_DIR/scripts/generate_synthetic_corpus.py" --output-dir "$OUT_DIR/synthetic"
"$PYTHON" "$SUITE_DIR/scripts/validate_gold_csv.py" "$OUT_DIR/synthetic/gold_records.csv" \
  --require-adjudicated --output "$OUT_DIR/gold_validation.json"
"$PYTHON" "$SUITE_DIR/scripts/freeze_run.py" --output-dir "$OUT_DIR/run" \
  --corpus-dir "$OUT_DIR/synthetic" --run-id offline-validation
"$PYTHON" "$SUITE_DIR/scripts/validate_manifest.py" "$OUT_DIR/run/run_manifest.json" \
  --schema "$SUITE_DIR/schemas/run_manifest.schema.json"
