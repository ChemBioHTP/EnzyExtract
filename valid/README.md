# Validation assets

This directory keeps validation material separate from production code. `RESULTS.md`
records the outcome of the most recent run; it is evidence, not a claim that an LLM
pipeline reproduced a published model's results.

## Task layout

- `../tests/valid/` — fast parser, dispatcher, and script contract tests. These use
  mocks and must not call an LLM or external service.
- `run_e2e_smoke.sh` — manually invoked live CLI smoke test. It needs `OPENAI_API_KEY`,
  an Entrez email address, a PDF corpus, and reachable external services. It is not CI.
- `validate_csv.py` — local structural validation of generated CSV files, including row
  counts, duplicate rows, numeric kinetic values, units, and a SHA-256 checksum.
- `corpus/` — place downloaded/public or explicitly labelled synthetic PDFs here. Keep
  a manifest with title, DOI/URL, expected terminal state, and licence/access notes.
- `download_test_articles.py` — builds the fixed 16-article smoke corpus (10
  validation-rendered PDFs, 1 native publisher PDF, and 5 independent JATS XML
  files) and its checksum manifest.
- `compare_model_runs.py` — compares canonical records across models and fails when
  any model emits records on declared negative-control documents.

## Default sequences path contract

Before a sequences-dir dispatcher change, leave `sequences --output-dir` unspecified.
The extractor therefore writes to `<enzy-root>/sequences`; `attach` must receive that
same path explicitly. The smoke script encodes this contract.

## Running the checks

Install the project and pytest in a supported Python environment, then run:

```bash
pytest tests/valid
python -m enzyextract --help
python -m enzyextract --enzy-root validation_runs/xml-smoke xml \
  --xml-root valid/corpus/articles/xml
bash valid/run_e2e_smoke.sh PDF_ROOT ENTREZ_EMAIL
```

Store `OPENAI_API_KEY=...` only in the repository-root `.env` file. Do not put a key
in a tracked script, fixture, report, or command history.

For a credential file stored elsewhere, pass it before the subcommand:

```bash
python -m enzyextract --env-file ~/.config/enzyextract/openai.env \
  --enzy-root validation_runs/live submit ...
```

The dotenv file may use quoted or unquoted values. Do not pass the secret itself as
a CLI argument because process listings and shell history can expose it.
