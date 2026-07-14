#!/usr/bin/env bash
# Live smoke test: requires OPENAI_API_KEY and external services.  Do not run in CI.
set -euo pipefail

PDF_ROOT="${1:?Usage: run_e2e_smoke.sh PDF_ROOT ENTREZ_EMAIL [NAMESPACE]}"
ENTREZ_EMAIL="${2:?Usage: run_e2e_smoke.sh PDF_ROOT ENTREZ_EMAIL [NAMESPACE]}"
NAMESPACE="${3:-smoke-v1}"
ENZY_ROOT="${ENZY_ROOT:-.enzy-smoke}"
MODEL_PATH="${REOCR_MODEL_PATH:-data/models/resnet18-remicro-iter3.pth}"

python -m enzyextract --help
enzyextract submit --help
enzyextract download --help
enzyextract sequences --help
enzyextract attach --help

enzyextract --enzy-root "$ENZY_ROOT" submit \
  --pdf-root "$PDF_ROOT" --namespace "$NAMESPACE" --mode confirm \
  --reocr-model-path "$MODEL_PATH"
enzyextract --enzy-root "$ENZY_ROOT" download \
  --namespace "$NAMESPACE" --output-csv kinetics.csv

# Keep the default <enzy-root>/sequences output while validating the dispatcher fix.
enzyextract --enzy-root "$ENZY_ROOT" sequences \
  --pdf-root "$PDF_ROOT" --entrez-email "$ENTREZ_EMAIL"
enzyextract --enzy-root "$ENZY_ROOT" attach \
  --download-csv kinetics.csv --sequences-dir "$ENZY_ROOT/sequences" \
  --output-csv final.csv

python "$(dirname "$0")/validate_csv.py" kinetics.csv --report kinetics_validation_report.json
python "$(dirname "$0")/validate_csv.py" final.csv --report final_validation_report.json
