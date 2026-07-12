from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).parents[2]
RUNNER = ROOT / "valid/poc/run_gpt5_live_validation.py"
REAL_RUNNER = ROOT / "valid/poc/run_gpt5_real_corpus_smoke.py"
TOOLS_PROBE = ROOT / "valid/poc/gpt5_responses_tools_probe.py"
STRICT_CRITERIA = ROOT / "valid/poc/strict_hallucination_criteria.json"


def source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_gpt5_runner_has_no_mock_or_replay_mode():
    text = source(RUNNER).lower()
    assert "allow-mock" not in text
    assert "--mock" not in text
    assert "mock_mode" not in text
    assert "replay-only" not in text
    assert "openai_api_key" in text


def test_gpt5_runner_invokes_real_enzyextract_submit_download_and_score():
    tree = ast.parse(source(RUNNER))
    constants = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
    joined = "\n".join(constants)
    assert "enzyextract" in joined
    assert "submit" in joined
    assert "interactive" in joined
    assert "download" in joined
    assert "score_extraction.py" in joined
    assert "evaluate_gates.py" in joined


def test_gpt5_runner_rejects_non_live_completion_jsonl():
    text = source(RUNNER)
    assert "assert_live_completion" in text
    assert "status_code" in text
    assert "body.get(\"id\")" in text
    assert "body.get(\"choices\")" in text


def test_strict_hallucination_criteria_is_zero_tolerance():
    text = source(STRICT_CRITERIA)
    assert '"document_level.negative_control_false_positive_documents"' in text
    assert '"safety.unsupported_record_count"' in text
    assert '"safety.catastrophic_numeric_error_count_1000x"' in text
    assert '"threshold": 0' in text


def test_responses_tools_probe_is_live_only_and_uses_responses():
    text = source(TOOLS_PROBE)
    assert "OPENAI_API_KEY is required" in text
    assert "client.responses.create" in text
    assert "web_search" in text
    assert "output_text" in text


def test_real_corpus_runner_requires_16_documents_and_no_fake_metrics():
    text = source(REAL_RUNNER)
    assert "len(rows) != 16" in text
    assert "len(pdf_rows) != 11" in text
    assert "len(xml_rows) != 5" in text
    assert "not_computed_no_adjudicated_gold_for_16_real_corpus" in text
    assert "OPENAI_API_KEY or --env-file is required" in text


def test_real_corpus_runner_invokes_pdf_submit_download_and_xml_preprocessing():
    tree = ast.parse(source(REAL_RUNNER))
    constants = [node.value for node in ast.walk(tree) if isinstance(node, ast.Constant) and isinstance(node.value, str)]
    joined = "\n".join(constants)
    assert "enzyextract" in joined
    assert "submit" in joined
    assert "interactive" in joined
    assert "download" in joined
    assert "xml" in joined
    assert "status_code" in source(REAL_RUNNER)
    assert "body.get(\"choices\")" in source(REAL_RUNNER)
