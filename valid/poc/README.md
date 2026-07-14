# GPT-5 live validation PoC

This folder contains live-only validation helpers for GPT-5 family models. They
must not be used as mocked or replay-only evidence.

## Run synthetic live validation with adjudicated gold

```bash
OPENAI_API_KEY=... /home/ranx/.conda/envs/py3/bin/python \
  valid/poc/run_gpt5_live_validation.py \
  --corpus-dir validation_offline/synthetic \
  --gold validation_offline/synthetic/gold_records.csv \
  --manifest validation_offline/synthetic/corpus_manifest.csv \
  --run-root validation_runs/gpt5-poc \
  --model openai/gpt-5.6-luna \
  --model openai/gpt-5.6-terra \
  --model openai/gpt-5.6-sol
```

The runner calls `python -m enzyextract submit --mode interactive`, requires a
real `OPENAI_API_KEY`, verifies that completion JSONL contains live 200-status
model responses, then scores with the same article-clustered scorer used by the
main validation suite.

This corpus has 7 synthetic documents and an adjudicated gold file. It is the
right corpus for precision, recall, F1, hallucination, and catastrophic-error
metrics.

## Run the 16-article real corpus smoke

```bash
OPENAI_API_KEY=... /home/ranx/.conda/envs/py3/bin/python \
  valid/poc/run_gpt5_real_corpus_smoke.py \
  --article-root valid/corpus/articles \
  --run-root validation_runs/gpt5-real-corpus-smoke \
  --model openai/gpt-5.6-luna \
  --model openai/gpt-5.6-terra \
  --model openai/gpt-5.6-sol
```

The real corpus currently has 16 documents: 11 PDF inputs and 5 XML inputs. This
runner validates live PDF extraction and XML preprocessing, but it does not report
precision, recall, or F1 because the 16 real articles do not yet have an
adjudicated gold file.

## Responses/tools probe

```bash
OPENAI_API_KEY=... /home/ranx/.conda/envs/py3/bin/python \
  valid/poc/gpt5_responses_tools_probe.py --model gpt-5.6-sol --enable-web-search
```

The probe is intentionally separate from extraction scoring. Web search can be
useful for metadata or external verification, but it can also violate the
document-only extraction rule and introduce unsupported records.
