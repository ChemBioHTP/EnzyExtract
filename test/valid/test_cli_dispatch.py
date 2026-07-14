"""Mocked dispatcher tests: no filesystem, network, or LLM calls."""
from __future__ import annotations

from types import SimpleNamespace

from enzyextract import cli


class DummyDataFrame:
    def __len__(self):
        return 0

    @property
    def columns(self):
        return []


def test_attach_forwards_explicit_sequences_dir(monkeypatch):
    captured = {}

    class Extractor:
        def step_5_attach_sequences(self, **kwargs):
            captured.update(kwargs)
            return DummyDataFrame()

    monkeypatch.setattr(cli, "_build_extractor", lambda args: Extractor())
    args = SimpleNamespace(download_csv="kinetics.csv", sequences_dir="custom_sequences",
                           output_csv="final.csv", use_llm=False)
    cli.cmd_attach_sequences(args)
    assert captured["sequences_dir"] == "custom_sequences"


def test_sequences_omits_output_dir_to_preserve_extractor_default(monkeypatch):
    captured = {}

    class Extractor:
        def step_4_fetch_sequences(self, **kwargs):
            captured.update(kwargs)
            return {"status": "mocked"}

    monkeypatch.setattr(cli, "_build_extractor", lambda args: Extractor())
    args = SimpleNamespace(pdf_root="pdfs", pmids_csv=None, entrez_email="test@example.org", output_dir=None)
    cli.cmd_fetch_sequences(args)
    assert captured["output_dir"] is None


def test_submit_forwards_all_cli_arguments(monkeypatch):
    captured = {}

    class Extractor:
        def submit_pdfs(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(cli, "_build_extractor", lambda args: Extractor())
    args = SimpleNamespace(pdf_root="pdfs", namespace="run-1", version="v2", mode="batch")
    cli.cmd_submit_pdfs(args)
    assert captured == {"pdf_root": "pdfs", "namespace": "run-1", "version": "v2", "mode": "batch"}


def test_download_forwards_output_path(monkeypatch):
    captured = {}

    class Extractor:
        def download_results(self, **kwargs):
            captured.update(kwargs)
            return DummyDataFrame()

    monkeypatch.setattr(cli, "_build_extractor", lambda args: Extractor())
    cli.cmd_download_results(SimpleNamespace(namespace="run-1", output_csv="out.csv"))
    assert captured == {"namespace": "run-1", "output_csv": "out.csv"}


def test_xml_preprocessing_forwards_root(monkeypatch):
    captured = {}

    class Extractor:
        def step_0_preprocess_xml(self, **kwargs):
            captured.update(kwargs)
            return DummyDataFrame()

    monkeypatch.setattr(cli, "_build_extractor", lambda args: Extractor())
    cli.cmd_preprocess_xml(SimpleNamespace(xml_root="xmls"))
    assert captured == {"xml_root": "xmls"}


def test_extractor_config_receives_private_env_file(monkeypatch):
    captured = {}

    class Extractor:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(cli, "EnzyExtract", Extractor)
    args = SimpleNamespace(
        reocr_model_path="model.pth", llm_name="openai/gpt-4o-mini",
        env_file="secrets/openai.env", enzy_root="run",
        skip_ocr=True, skip_tables=True,
    )
    cli._build_extractor(args)
    assert captured["config"].env_file == "secrets/openai.env"
    assert captured["config"].llm_name == "openai/gpt-4o-mini"
    assert captured["config"].skip_ocr is True
    assert captured["config"].skip_tables is True
