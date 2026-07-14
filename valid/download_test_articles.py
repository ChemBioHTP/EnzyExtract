#!/usr/bin/env python3
"""Download a fixed, auditable 16-article open-access smoke corpus."""
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import io
import re
import tarfile
import textwrap
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas


PDF_ARTICLES = [
    ("PMC11360556", "38888314", "10.1128/jmbe.00050-24", "Educational activity of enzyme kinetics in an undergraduate biochemistry course"),
    ("PMC10614204", "37902256", "10.1002/prp2.1149", "CYP2C19 and CYP3A4 metabolism of beta-eudesmol: reaction phenotyping and enzyme kinetics"),
    ("PMC10713844", "38090711", "10.3389/fbioe.2023.1296880", "Characterization of EMP and HMP pathway enzyme kinetics in Corynebacterium glutamicum"),
    ("PMC9053035", "35496683", "10.1128/jmbe.00286-21", "Development and Implementation of a Remote Enzyme Kinetics Laboratory Exercise"),
    ("PMC10051508", "36984943", "10.3390/mi14030537", "Miniaturised fluidic system for analysis of enzyme kinetics"),
    ("PMC10480237", "37421557", "10.1007/s10930-023-10132-6", "HIV protease hinge insertions affect enzyme kinetics and stability"),
    ("PMC10087753", "35997626", "10.1111/febs.16602", "GH7 cellobiohydrolase enzyme kinetics on chromogenic substrates"),
    ("PMC9730296", "36417687", "10.1021/acs.analchem.2c03164", "High-throughput steady-state enzyme kinetics in droplets"),
    ("PMC10652518", "37974243", "10.1186/s13104-023-06618-2", "Enzyme kinetics of deoxyuridine triphosphatase from Western corn rootworm"),
    ("PMC9335329", "35867821", "10.1073/pnas.2206588119", "EGFR exon 19 deletion activation, enzyme kinetics, and inhibitor sensitivities"),
]

XML_ARTICLES = [
    ("PMC11662684", "39588774", "10.1093/nar/gkae1124", "Interpreting CRISPR-Cas12a enzyme kinetics through free energy change of nucleic acids"),
    ("PMC11355918", "39195489", "10.3390/md22080373", "Inhibition of Soluble Epoxide Hydrolase by Cembranoid Diterpenes"),
    ("PMC11745417", "39752624", "10.1371/journal.pcbi.1012162", "Biomathematical enzyme kinetics model of prebiotic autocatalytic RNA networks"),
    ("PMC12267109", "40671269", "10.1002/pro.70229", "The enzyme kinetics of branched-chain fatty acid synthesis"),
    ("PMC12267849", "40670369", "10.1038/s41467-025-61631-2", "Probing the modulation of enzyme kinetics by multi-temperature serial crystallography"),
]

DIRECT_PDF_ARTICLES = [
    (
        "nature-s42003-019-0365-y",
        "s42003-019-0365-y.pdf",
        "",
        "10.1038/s42003-019-0365-y",
        "Arsinothricin, an arsenic-containing non-proteinogenic amino acid analog of glutamate, is a broad-spectrum antibiotic",
        "https://www.nature.com/articles/s42003-019-0365-y.pdf",
        "Nature publisher PDF downloaded for validation coverage; open-access article",
    ),
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "EnzyExtract-validation/2.0 (open-access test corpus)"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def pdf_url(pmcid: str) -> str:
    page = get(f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}/").decode("utf-8", "replace")
    match = re.search(r'<meta\s+name="citation_pdf_url"\s+content="([^"]+)"', page)
    if not match:
        raise RuntimeError(f"PMC did not advertise a PDF for {pmcid}")
    return html.unescape(match.group(1))


def oa_package_url(pmcid: str) -> tuple[str, str]:
    payload = get(f"https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi?id={pmcid}")
    root = ET.fromstring(payload)
    record = root.find(".//record")
    link = root.find(".//link[@format='tgz']")
    if record is None or link is None or not link.get("href"):
        raise RuntimeError(f"No PMC open-access package is available for {pmcid}")
    return link.get("href", "").replace("ftp://", "https://"), record.get("license", "unspecified")


def download_pdf(pmcid: str, destination: Path) -> tuple[str, str]:
    package_url, license_name = oa_package_url(pmcid)
    archive = get(package_url)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        candidates = [member for member in bundle.getmembers() if member.isfile() and member.name.lower().endswith(".pdf")]
        if not candidates:
            raise RuntimeError(f"The PMC package for {pmcid} contains no PDF")
        member = min(candidates, key=lambda item: (item.name.count("/"), len(item.name)))
        extracted = bundle.extractfile(member)
        if extracted is None:
            raise RuntimeError(f"Could not extract {member.name} for {pmcid}")
        payload = extracted.read()
    if not payload.startswith(b"%PDF"):
        raise RuntimeError(f"Extracted content for {pmcid} is not a PDF")
    destination.write_bytes(payload)
    return package_url, license_name


def download_xml(pmcid: str, destination: Path) -> str:
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    payload = get(url)
    root = ET.fromstring(payload)
    if root.tag.rsplit("}", 1)[-1] not in {"article", "articles"}:
        raise RuntimeError(f"Unexpected XML root for {pmcid}: {root.tag}")
    destination.write_bytes(payload)
    return url


def download_direct_pdf(url: str, destination: Path) -> str:
    payload = get(url)
    if not payload.startswith(b"%PDF"):
        raise RuntimeError(f"Downloaded content from {url} is not a PDF")
    destination.write_bytes(payload)
    return url


def render_jats_pdf(pmcid: str, destination: Path) -> str:
    """Render Europe PMC JATS text to a deterministic PDF for CLI smoke testing."""
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    payload = get(url)
    root = ET.fromstring(payload)
    text = " ".join(part.strip() for part in root.itertext() if part.strip())
    if len(text) < 500:
        raise RuntimeError(f"Full text for {pmcid} is unexpectedly short")
    font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")
    font_name = "Helvetica"
    if font_path.exists():
        if "ValidationJATS" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("ValidationJATS", str(font_path)))
        font_name = "ValidationJATS"
    pdf = canvas.Canvas(str(destination))
    pdf.setTitle(f"{pmcid} - derived validation rendering")
    y = 800
    pdf.setFont(font_name, 8)
    header = f"VALIDATION RENDERING FROM EUROPE PMC JATS XML — {pmcid}"
    for paragraph in [header, text]:
        for line in textwrap.wrap(paragraph, width=105, break_long_words=False, break_on_hyphens=False):
            if y < 42:
                pdf.showPage()
                pdf.setFont(font_name, 8)
                y = 800
            pdf.drawString(36, y, line)
            y -= 10
        y -= 5
    pdf.save()
    return url


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("valid/corpus/articles"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir = args.output_dir / "pdf"
    xml_dir = args.output_dir / "xml"
    pdf_dir.mkdir(exist_ok=True)
    xml_dir.mkdir(exist_ok=True)
    rows = []
    for pmcid, pmid, doi, title in PDF_ARTICLES:
        destination = pdf_dir / f"{pmcid}.pdf"
        if args.force or not destination.exists():
            url = render_jats_pdf(pmcid, destination)
        else:
            url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
        rows.append({
            "document_id": pmcid, "filename": destination.name, "pmcid": pmcid,
            "format": "pdf_derived_from_jats", "relative_path": destination.relative_to(args.output_dir).as_posix(),
            "pmid": pmid, "doi": doi, "title": title, "source_url": url,
            "license_access": "Validation-only PDF rendering of Europe PMC open-access JATS; not publisher layout",
            "sha256": sha256(destination), "bytes": destination.stat().st_size,
            "expected_terminal_status": "processed_or_skipped_with_reason",
        })
        print(f"{pmcid}: {destination.stat().st_size} bytes")
    for document_id, filename, pmid, doi, title, source_url, license_access in DIRECT_PDF_ARTICLES:
        destination = pdf_dir / filename
        url = download_direct_pdf(source_url, destination) if args.force or not destination.exists() else source_url
        rows.append({
            "document_id": document_id, "filename": destination.name, "pmcid": "",
            "format": "pdf", "relative_path": destination.relative_to(args.output_dir).as_posix(),
            "pmid": pmid, "doi": doi, "title": title, "source_url": url,
            "license_access": license_access,
            "sha256": sha256(destination), "bytes": destination.stat().st_size,
            "expected_terminal_status": "processed_or_skipped_with_reason",
        })
        print(f"{document_id}: {destination.stat().st_size} bytes")
    for pmcid, pmid, doi, title in XML_ARTICLES:
        destination = xml_dir / f"{pmcid}.xml"
        url = (download_xml(pmcid, destination) if args.force or not destination.exists()
               else f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML")
        ET.parse(destination)
        rows.append({
            "document_id": pmcid, "filename": destination.name, "pmcid": pmcid,
            "format": "xml", "relative_path": destination.relative_to(args.output_dir).as_posix(),
            "pmid": pmid, "doi": doi, "title": title, "source_url": url,
            "license_access": "Europe PMC full-text XML from an open-access article",
            "sha256": sha256(destination), "bytes": destination.stat().st_size,
            "expected_terminal_status": "processed_or_skipped_with_reason",
        })
        print(f"{pmcid}: {destination.stat().st_size} bytes")
    manifest = args.output_dir / "corpus_manifest.csv"
    with manifest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {manifest} with {len(rows)} articles (11 PDF, 5 XML)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
