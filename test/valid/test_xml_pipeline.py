from __future__ import annotations

from enzyextract.pre.scans.scan_to_parquet import scan_xmls_by_folder


def test_jats_xml_preprocessing_preserves_text_and_tables(tmp_path):
    (tmp_path / "D1.xml").write_text(
        """<?xml version="1.0"?><article><front><article-meta><abstract><p>Enzyme abstract.</p></abstract></article-meta></front>
        <body><sec><p>Measured kcat was 9 s-1.</p><table-wrap><table><tr><td>Km</td><td>12 µM</td></tr></table></table-wrap></sec></body></article>""",
        encoding="utf-8",
    )
    frame = scan_xmls_by_folder(tmp_path)
    assert frame.height == 1
    assert frame["pmid"][0] == "D1"
    assert "kcat" in frame["content"][0]
    assert frame["abstract"][0] == "Enzyme abstract."
    assert len(frame["tables"][0]) == 1
