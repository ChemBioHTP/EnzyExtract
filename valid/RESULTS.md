# Validation results — 2026-07-12

## Scope and branch

Validation was run on branch `DTZ/test` at `829a067` (the branch initially pointed
to the same commit as local `main` and `origin/main`). This run covers CLI contracts,
offline scoring, a 16-article PDF/XML smoke corpus, synthetic adversarial fixtures,
and live two-model synthetic extraction. It does not claim locked-holdout scientific validity.

The requested conda environment was `/home/ranx/.conda/envs/py3` (Python 3.10.12).
The repository `.env` contains a working `OPENAI_API_KEY`; authentication succeeded
after the credential was updated. `ENTREZ_EMAIL` was not present.

## Pipeline execution order

This validation used five ordered pipelines. Four produced completed validation
evidence; the optional sequence-enrichment pipeline was not run live because an
Entrez email was unavailable.

1. **Fast local contract tests**: run `pytest test/valid` to validate CLI parsing,
   command dispatch, error decoding, XML preprocessing, and validation utilities
   without network or LLM calls. Result: 16 tests passed.
2. **Offline synthetic-artifact validation**: run `bash valid/run_offline_validation.sh`.
   It runs the repository tests and validation-suite tests, generates seven labelled
   synthetic PDFs, validates the eight-record adjudicated gold CSV, and freezes and
   schema-validates a run manifest. Result: completed.
3. **Sixteen-article input smoke pipeline**: build the fixed corpus, then preprocess
   its PDF and XML branches. The corpus has **16 articles: 11 PDF inputs** (10 JATS
   renderings and 1 native publisher PDF) **and 5 JATS XML inputs**. PDF inputs use
   `submit`; XML inputs use `xml`. Result: all 16 inputs reached a recorded terminal
   preprocessing state. The full article manifest is
   `valid/corpus/articles/corpus_manifest.csv`.
4. **Live synthetic extraction and scoring**: for each model, run `submit --mode
   interactive`, confirm live completion JSONL, run `download`, then score against
   the adjudicated synthetic gold CSV and apply strict hallucination/error gates.
   Result: the scored outputs and metrics are listed below and stored under
   `validation_runs/live*/scoring/` and `validation_runs/live5-gpt56-luna/scoring/`.
5. **Optional sequence enrichment**: after a downloaded kinetics CSV exists, run
   `sequences` and then `attach`. Result: not run live because `ENTREZ_EMAIL` was
   unavailable; mocked CLI and dispatcher tests passed.

The exact environment requirements and copyable CLI commands for these pipelines
are in `valid/valid_requiremnt.txt`. This report is the authoritative location for
the article count, terminal states, metrics, limitations, and artifact paths.

## Results

| Endpoint | Result | Evidence / limitation |
| --- | --- | --- |
| Repository CLI/validation tests | PASS: 16 passed | Parser, configurable private env file, all five dispatch targets (`submit`, `xml`, `download`, `sequences`, `attach`), error-response decoding, JATS parsing, model comparison, and validation utilities. |
| Validation suite tests | PASS: 19 passed | Schema, scorer, article bootstrap, CLI, raw-unit canonicalization, mutant aliases, 10x/100x/1000x thresholds, and micro/milli checks. |
| Offline validation wrapper | PASS | Generated 7 clearly labelled synthetic PDFs, 8 adjudicated gold records, a valid gold CSV, and a valid frozen run manifest. |
| Synthetic corpus composition | PASS | 2 positive documents with 8 gold records plus 5 negative/decoy documents: review, nonbiological catalyst, antibody binding, ASR, and prompt injection. |
| Article-clustered bootstrap | PASS (identity self-check only) | 2,000 article-level resamples, seed 20260712; strict precision/recall/F1 and all CI bounds were 1.0 when gold was replayed as prediction. This is a scorer check, not model performance. |
| Catastrophic error outputs | PASS (code/self-check) | Separate cumulative counts/rates for >=10-fold, >=100-fold, and >=1000-fold errors, plus unflagged approximately-1000-fold micro/milli errors. Identity self-check returned zero for all. |
| Real-article corpus | PASS: 16/16 acquired | 16 distinct open-access articles with DOI/URL/source/checksum manifest: 10 validation PDFs rendered from JATS, 1 native publisher PDF, and 5 independent source JATS XML files. |
| PDF preprocessing | PASS: 10/10 | OCR completed, table detector examined all inputs, PDF text scan produced 66 page rows, OCR produced 101 m/M candidates, and a 10-item batch JSONL was created. No tables were detected in the plain-text validation renderings. |
| XML preprocessing CLI | PASS: 5/5 | `enzyextract xml` wrote 5 parquet rows; body and abstract were non-empty for all; 5 JATS tables were retained across 3 articles. |
| Input preprocessing terminal status | PASS: 16/16 | Eleven PDF inputs reached `batch_ready_for_confirmation`; five XML inputs reached `processed`. LLM completion is tracked separately. |
| No hidden OCR weight download | PASS | Inference now loads the supplied custom ResNet checkpoint with `weights=None`; ImageNet weights are no longer requested. |
| Skip-OCR CLI path | PASS (local validation only) | `--skip-ocr` bypasses mM preprocessing and table micro-correction. On the 7-doc synthetic corpus, the generated request bodies were identical to the default-OCR batch, while preprocessing dropped from roughly 2:42 total OCR/table time to roughly 7 s table-only time. |
| Live gpt-4o-mini synthetic smoke | COMPLETE: 7/7 terminal | 11 standardized parameter records: strict TP/FP/FN 6/5/2; precision 0.545, recall 0.750, F1 0.632. Article-bootstrap 95% CIs: precision [0, 0.875], recall [0, 1.0], F1 [0, 0.923]. |
| Live gpt-4o synthetic smoke | COMPLETE: 7/7 terminal | 2 standardized records, both from the secondary-review negative control; strict TP/FP/FN 0/2/8 and precision/recall/F1 0. |
| Live gpt-5.6-luna synthetic smoke | COMPLETE: 7/7 terminal | 8 standardized parameter records from a real EnzyExtract API run: strict TP/FP/FN 7/1/1; precision 0.875, recall 0.875, F1 0.875. Article-bootstrap 95% CIs: precision [0.75, 1.0], recall [0.75, 1.0], F1 [0.75, 1.0]. Negative-control false-positive documents: 0. |
| Live gpt-5.6-terra synthetic smoke | COMPLETE: 7/7 terminal | 8 standardized parameter records from a real EnzyExtract API run: strict TP/FP/FN 7/1/1; precision 0.875, recall 0.875, F1 0.875. Negative-control false-positive documents: 0. Strict hallucination/error gates still failed because unsupported_record_count=1 and catastrophic_numeric_error_count_1000x=1. |
| Live gpt-5.6-sol synthetic smoke | COMPLETE: 7/7 terminal | 8 standardized parameter records from a real EnzyExtract API run: strict TP/FP/FN 7/1/1; precision 0.875, recall 0.875, F1 0.875. The successful completion rows have `chatcmpl-*` IDs; the key fix was removing incompatible `max_tokens=None` from the Chat Completions request path. Strict hallucination/error gates still failed because unsupported_record_count=1 and catastrophic_numeric_error_count_1000x=1. |
| GPT-5.6 Responses API probe | PASS | `gpt-5.6-sol` reached the live Responses API and returned response IDs `resp_045cfbf3c440a0ed006a53eefafd6881a09a0f171a1293a1da` and `resp_0dcf068a2f9370ca006a53ef1015d481a0b5a7b20d4e2aa613`. The second request enabled `web_search`; the model did not call it because the probe did not require external information. |
| 16-article GPT-5.6 real-corpus smoke | INTERRUPTED BEFORE API | The run was started against the 16-article corpus but was interrupted during local mM OCR at 4/11 PDFs after roughly 3:45. No batch or completion JSONL had been written, so the OpenAI API had not yet been called for that run. |
| Cross-model difference | COMPLETE | 2 shared records, 9 mini-only, 0 gpt-4o-only; canonical-record Jaccard 0.182. The two shared records are the unsupported review values. |
| Live hallucination/negative-control rate | FAIL both models | gpt-4o-mini emitted 3 unsupported records across review and nonbiological-catalyst documents; gpt-4o emitted 2 unsupported review records. Binding, ASR, and prompt-injection controls produced no final records. |
| Strict gpt-5.6-luna hallucination/error gate | FAIL | Negative controls were clean, but one positive-document prediction remained unsupported and had a >=1000-fold numeric error. Strict validation now treats unsupported records and hallucination-like errors with zero tolerance. |
| Catastrophic live errors | FAIL gpt-4o-mini | Two P02 kcat errors were >=1000-fold (8,000x and 10,000x); >=10x, >=100x, and >=1000x counts were each 2. Unflagged approximately-1000x micro/milli count was 0. gpt-4o emitted no positive-document values and therefore had zero magnitude errors but eight false negatives. |
| Stored-response replay | PASS both models | Canonical CSV replay was byte-identical for each model. |
| Diagnostic synthetic gates | FAIL | gpt-4o-mini: 3 critical and 5 major failures. gpt-4o: 5 critical and 5 major failures. These are engineering diagnostics on seven synthetic documents, not a locked-holdout release decision. |
| Sequence/attach live chain | NOT RUN | `.env` lacks `ENTREZ_EMAIL`; these paths are covered by mocked argument/dispatcher tests only. |
| Locked holdout acceptance decision | NOT RUN | No locked gold/prediction corpus was supplied. Proposed thresholds remain frozen; the diagnostic synthetic failures do not authorize changing them. |

## Minor findings

- Credential usability was too implicit because the CLI always loaded repository-root
  `.env`. The CLI now accepts a global `--env-file PATH`, for example
  `--env-file ~/.config/enzyextract/openai.env`, while retaining `.env` as the
  default. Quoted dotenv values are supported; placing the secret itself in a CLI
  argument is intentionally not supported because it can leak through process lists
  and shell history.
- An initial credential parsed correctly but failed provider authentication; after
  its value was updated, read-only authentication and both live model runs succeeded.
  Quoting was not the cause of the initial failure.
- The `gpt-5.6` live attempt in this environment currently fails with
  `APIConnectionError: Connection error` for both `chat.completions` and
  `responses`, so there is no new live 5.6 metric row to compare against the
  earlier `gpt-4o` runs. This is an environment transport issue, not a decoding
  or scorer issue.

## Issue

- `gpt-4o` and `gpt-4o-mini` outputs are model baselines, not EnzyExtract ground
  truth. They must not be used as gold labels for GPT-5.6 scoring. Precision,
  recall, F1, hallucination rate, and catastrophic-error counts are only valid
  when scored against an adjudicated gold file such as
  `validation_offline/synthetic/gold_records.csv`.
- Earlier GPT-5.6 EnzyExtract failures were caused by the synchronous OpenAI path
  passing Chat Completions parameters that GPT-5.6 rejects. The current sync path
  omits null `max_tokens`, retries with GPT-5-compatible completion-token
  parameters when needed, and keeps a Responses API fallback for permission or
  endpoint differences. The scored `gpt-5.6-sol` run completed through Chat
  Completions after that compatibility fix.
- The current live metrics are diagnostic, not release-grade. They are based on a
  7-document synthetic corpus and do not represent the locked holdout.
- The 16-article real PDF/XML corpus is a CLI smoke corpus. Until it receives an
  adjudicated gold file, it can report terminal status, extracted-row counts, and
  unsupported-record sanity checks, but not strict precision/recall/F1.
- The 7/7 runs are intentionally the synthetic scored benchmark, not the 16-article
  real corpus. They are the only current corpus with adjudicated gold records, so
  they are the only live runs in this report that can produce precision, recall,
  F1, article-clustered CIs, and catastrophic-error metrics.
- A zero API-spend display during the interrupted 16-article run is expected: the
  run was still in local OCR/table preprocessing and had not reached OpenAI
  submission. The small Responses probes above are real API calls, but dashboard
  usage may lag and the token volume is tiny.
- Hallucination scoring is strict on declared negative controls, but it can still
  miss unsupported records on positive documents if they are not explicitly
  enumerated as wrong-row failures.
- Article-cluster bootstrap reduces within-paper leakage, but with a very small
  corpus the confidence intervals are still wide and are not a substitute for a
  larger locked holdout.
- Exact canonicalization is useful for stable comparison, but it can hide format
  drift inside semantically similar rows. That is a metrics limitation, not an
  implementation bug.
- The added Nature PDF is a native publisher PDF, so corpus coverage now spans
  both JATS-rendered validation PDFs and real publisher layout. That improves
  coverage, but it also means OCR and layout behavior are no longer homogeneous
  across the smoke corpus.

## Material defects found and repaired

1. `attach --sequences-dir` was forwarded to the extractor.
2. XML preprocessing passed the wrong keyword (`xmls_folder`) and had no CLI entry;
   it now uses `xml_folder` and is exposed as `enzyextract xml --xml-root`.
3. XML parsing used HTML mode and silently missed standard JATS abstracts/tables;
   JATS and Elsevier-style tags are now supported in XML mode.
4. Custom OCR inference unnecessarily downloaded ImageNet weights before loading the
   repository checkpoint; inference is now offline-safe.
5. OpenAI error responses with a null body crashed batch decoding; they now retain
   terminal status/error metadata and produce an explicit no-valid-records summary.
6. The offline wrapper accidentally invoked system Python and combined duplicate test
   module names in one pytest process; it now uses `$PYTHON -m pytest` in two runs.
7. Synchronous API failures were recorded as HTTP 500 regardless of provider status;
   future completion rows now retain the exception's actual status code, such as 401.
8. Wide CLI outputs contained raw values but no `kcat_value`/`km_value`; the scorer
   now canonicalizes µM, nM, mM, M, s^-1, and min^-1 directly from raw fields.
9. The EnzyExtract prediction map referenced absent `clean_mutant`; it now uses the
   actual `mutant` column, and `WT`/`wild-type` aliases are canonicalized.
10. Synthetic gold used `synthetic` as an organism and assigned WT where no variant
    was stated. Unknown organisms/mutants are now blank rather than enforced metadata.
11. `--skip-ocr` now exists as a CLI option; on the current synthetic corpus it
    preserves the generated model request bodies while bypassing the mM OCR pass.

## Artifacts

- `valid/corpus/articles/corpus_manifest.csv` — 16-article source and checksum manifest.
- `validation_runs/xml-smoke-3/scans/xml/xml.parquet` — real XML preprocessing output.
- `validation_runs/pdf-smoke-2/scans/pdf/pdf.parquet` — 66-page PDF scan.
- `validation_runs/pdf-smoke-2/batches/validation-15_v1.jsonl` — 10-item pre-submit batch.
- `validation_offline/synthetic/` — seven synthetic adversarial PDFs and gold records.
- `validation_offline/scoring-self-check/metrics.json` — 2,000-bootstrap scorer self-check.
- `validation_runs/live2-gpt4o-mini/scoring/metrics.complete.json` and `gate_report.json`.
- `validation_runs/live2-gpt4o/scoring/metrics.complete.json` and `gate_report.json`.
- `validation_runs/model_comparison_live2.json` — canonical pairwise/hallucination comparison.

## Environment caveats

Installing current EnzyExtract dependencies into the shared `py3` environment upgraded
Torch/Torchvision and exposed pre-existing dependency conflicts with BioEmu, Protenix,
and EnzyHTP packages in that same environment. EnzyExtract imports and validation pass,
but those unrelated packages were not regression-tested. `polars[rtcompat]` was installed
because the login CPU lacks an instruction required by the default Polars runtime.

No GPU or SLURM job was needed. Live synthetic completions were generated successfully,
but neither model passed the diagnostic acceptance gates. No locked-holdout
model-performance acceptance gate should be interpreted as passed.
