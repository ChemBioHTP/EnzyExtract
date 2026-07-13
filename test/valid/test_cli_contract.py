"""CLI parser contract tests; these do not invoke pipeline work."""
from __future__ import annotations

import pytest

from enzyextract import cli


def parse(argv: list[str]):
    return cli.build_parser().parse_args(argv)


def test_documented_workflow_parses():
    assert parse(["--enzy-root", ".enzy-smoke", "sequences", "--pdf-root", "pdfs",
                  "--entrez-email", "tester@example.org"]).output_dir is None
    attach = parse(["attach", "--download-csv", "kinetics.csv", "--sequences-dir", ".enzy-smoke/sequences"])
    assert attach.sequences_dir == ".enzy-smoke/sequences"
    assert parse(["xml", "--xml-root", "xmls"]).xml_root == "xmls"
    assert parse(["--env-file", "secrets/openai.env", "download"]).env_file == "secrets/openai.env"
    submit = parse(["--no-ocr", "--no-tables", "submit", "--pdf-root", "pdfs"])
    assert submit.skip_ocr is True
    assert submit.skip_tables is True
    assert parse(["--skip-ocr", "submit", "--pdf-root", "pdfs"]).skip_ocr is True


def test_required_arguments_fail_at_parse_time():
    with pytest.raises(SystemExit):
        parse(["sequences", "--pdf-root", "pdfs"])
    with pytest.raises(SystemExit):
        parse(["attach", "--download-csv", "kinetics.csv"])
    with pytest.raises(SystemExit):
        parse(["xml"])
