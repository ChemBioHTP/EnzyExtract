"""Static and local tests for the scripts under ``valid``."""
from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).parents[2]
VALID = ROOT / "valid"


def load_validator():
    spec = importlib.util.spec_from_file_location("validate_csv", VALID / "validate_csv.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_csv_validator_exposes_a_sha256_helper(tmp_path):
    validator = load_validator()
    csv_file = tmp_path / "output.csv"
    csv_file.write_text("pmid,kcat\n1,2 s^-1\n", encoding="utf-8")
    assert len(validator.sha256(csv_file)) == 64


def test_smoke_script_uses_default_sequence_output_and_explicit_attach_input():
    script = (VALID / "run_e2e_smoke.sh").read_text(encoding="utf-8")
    assert '--entrez-email "$ENTREZ_EMAIL"' in script
    assert '--output-dir "$ENZY_ROOT/sequences"' not in script
    assert '--sequences-dir "$ENZY_ROOT/sequences"' in script


def test_real_corpus_plan_has_15_unique_articles_in_two_formats():
    spec = importlib.util.spec_from_file_location("download_test_articles", VALID / "download_test_articles.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    ids = [row[0] for row in module.PDF_ARTICLES + module.XML_ARTICLES]
    assert len(module.PDF_ARTICLES) == 10
    assert len(module.XML_ARTICLES) == 5
    assert len(ids) == len(set(ids)) == 15


def test_custom_reocr_checkpoint_does_not_download_imagenet_weights():
    source = (ROOT / "enzyextract" / "pre" / "reocr" / "m_mu_reocr.py").read_text(encoding="utf-8")
    inference = source[source.index("def resnet_reocr_milli"):source.index("def reocr_all_mM")]
    assert "resnet18(weights=None)" in inference
    assert "ResNet18_Weights.DEFAULT" not in inference
