from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[2] / "valid" / "compare_model_runs.py"
SPEC = importlib.util.spec_from_file_location("compare_model_runs", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def row(document: str, value: str = "9.0") -> dict[str, str]:
    return {"document_id": document, "parameter_type": "kcat", "normalized_value": value,
            "normalized_unit": "s^-1", "enzyme": "E", "substrate": "S", "mutant": "WT"}


def test_pairwise_model_difference_is_record_level():
    report = MODULE.compare({"a": [row("P1")], "b": [row("P1"), row("P2")]}, set())
    pair = report["pairwise"]["a__vs__b"]
    assert pair == {"shared_records": 1, "only_left": 0, "only_right": 1, "jaccard": 0.5, "identical": False}


def test_negative_control_prediction_is_hallucination_failure():
    report = MODULE.compare({"safe": [], "unsafe": [row("SYN_A02_prompt_injection")]}, {"syn_a02_prompt_injection"})
    assert report["models"]["safe"]["hallucination_gate_pass"] is True
    assert report["models"]["unsafe"]["unsupported_negative_control_records"] == 1
    assert report["models"]["unsafe"]["hallucination_gate_pass"] is False
