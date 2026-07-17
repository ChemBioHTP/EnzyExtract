"""
Command-line interface for EnzyExtract.

Usage:
    enzyextract submit              Preprocess PDFs or XMLs and submit LLM batch
    enzyextract download            Download batch results and convert to DataFrame
    enzyextract sequences           Scan PDFs for accession IDs & fetch sequences
    enzyextract attach              Attach sequences to GPT-extracted data
"""

import argparse
import sys
from typing import Optional, Sequence

from enzyextract.extractor.extractor import EnzyExtract, EnzyExtractConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="enzyextract",
        description="EnzyExtract: Extract enzyme data from scientific PDFs using LLMs.",
    )

    # --- global options mirroring EnzyExtractConfig ---
    parser.add_argument(
        "--llm-name",
        default="openai/gpt-4o",
        help="LLM model name (default: openai/gpt-4o)",
    )
    parser.add_argument(
        "--enzy-root",
        default=".enzy",
        help="Working directory for intermediate files (default: .enzy)",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Private dotenv file containing provider credentials (default: .env)",
    )
    parser.add_argument(
        "--skip-ocr",
        action="store_true",
        help="Skip mM OCR preprocessing and table-level micro correction",
    )
    parser.add_argument(
        "--skip-tables",
        action="store_true",
        help="Skip table extraction preprocessing",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- submit ---
    submit_parser = subparsers.add_parser(
        "submit",
        help="Preprocess PDFs or XMLs and create LLM batch files",
    )
    submit_parser.add_argument(
        "--reocr-model-path",
        default="data/models/resnet18-remicro-iter3.pth",
        help="Path to the ResNet18 mM OCR model (default: data/models/resnet18-remicro-iter3.pth)",
    )
    submit_parser.add_argument(
        "--pdf-root",
        default=None,
        help="Directory containing PDF files to process",
    )
    submit_parser.add_argument(
        "--xml-root",
        default=None,
        help="Directory containing XML full-text files to process",
    )
    submit_parser.add_argument(
        "--namespace",
        default="default-namespace",
        help="Namespace for the LLM batch (default: default-namespace)",
    )
    submit_parser.add_argument(
        "--version",
        default=None,
        help="Optional version string (e.g. 'v1')",
    )
    submit_parser.add_argument(
        "--mode",
        default="interactive",
        choices=["interactive", "batch", "confirm"],
        help="Submission mode: 'interactive' runs serially, 'batch' creates OpenAI batch files, 'confirm' requires manual confirmation",
    )

    xml_parser = subparsers.add_parser(
        "xml",
        help="Preprocess JATS/XML full text into an EnzyExtract parquet scan",
    )
    xml_parser.add_argument(
        "--xml-root",
        required=True,
        help="Directory containing XML full-text files",
    )

    # --- download ---
    download_parser = subparsers.add_parser(
        "download",
        help="Download batch results from OpenAI and convert to a DataFrame",
    )
    download_parser.add_argument(
        "--namespace",
        default="default-namespace",
        help="Namespace for the LLM batch (default: default-namespace)",
    )
    download_parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional path to write results as a CSV file",
    )

    # --- sequences ---
    sequences_parser = subparsers.add_parser(
        "sequences",
        help="Scan PDFs for accession IDs (PDB, UniProt, RefSeq, GenBank) and fetch sequences",
    )
    sequences_parser.add_argument(
        "--pdf-root",
        required=True,
        help="Directory containing PDF files to scan",
    )
    sequences_parser.add_argument(
        "--pmids-csv",
        default=None,
        help="Optional CSV with a 'pmid' column; UniProt entries linked to those PMIDs are also fetched",
    )
    sequences_parser.add_argument(
        "--entrez-email",
        required=True,
        help="Email address required by NCBI Entrez (BioPython) for sequence fetching",
    )
    sequences_parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write the fetched sequences parquet files (default: <enzy-root>/sequences)",
    )

    # --- attach ---
    attach_parser = subparsers.add_parser(
        "attach",
        help="Attach enzyme sequences to GPT-extracted data via string-similarity matching",
    )
    attach_parser.add_argument(
        "--download-csv",
        required=True,
        help="Path to the CSV produced by the 'download' subcommand",
    )
    attach_parser.add_argument(
        "--sequences-dir",
        required=True,
        help="Directory containing the parquet files written by the 'sequences' subcommand",
    )
    attach_parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional path to write the enriched DataFrame as a CSV file",
    )
    attach_parser.add_argument(
        "--use-llm",
        action="store_true",
        default=False,
        help="Use an LLM to confirm / improve accession matching",
    )

    return parser


def _build_extractor(args: argparse.Namespace) -> EnzyExtract:
    """Build an EnzyExtract instance from parsed CLI arguments."""
    reocr_model_path = getattr(args, "reocr_model_path", "data/models/resnet18-remicro-iter3.pth")
    config = EnzyExtractConfig(
        reocr_model_path=reocr_model_path,
        llm_name=args.llm_name,
        env_file=args.env_file,
        skip_ocr=getattr(args, "skip_ocr", False),
        skip_tables=getattr(args, "skip_tables", False),
    )
    extractor = EnzyExtract(
        enzy_root=args.enzy_root,
        config=config,
    )
    return extractor


def cmd_submit(args: argparse.Namespace) -> None:
    """Handle the 'submit' subcommand."""
    extractor = _build_extractor(args)

    if args.xml_root:
        print(f"Submitting XMLs (namespace={args.namespace}, mode={args.mode})...")
        extractor.submit_xmls(
            xml_root=args.xml_root,
            namespace=args.namespace,
            version=args.version,
            mode=args.mode,
        )
    elif args.pdf_root:
        print(f"Submitting PDFs (namespace={args.namespace}, mode={args.mode})...")
        extractor.submit_pdfs(
            pdf_root=args.pdf_root,
            namespace=args.namespace,
            version=args.version,
            mode=args.mode,
        )
    else:
        print("Error: Either --pdf-root or --xml-root must be provided.")
        print("Usage: enzyextract submit --pdf-root <path> | --xml-root <path> [options]")
        import sys
        sys.exit(1)
    print("Done.")


def cmd_download_results(args: argparse.Namespace) -> None:
    """Handle the 'download' subcommand."""
    extractor = _build_extractor(args)
    print(f"Downloading results (namespace={args.namespace})...")
    df = extractor.download_results(namespace=args.namespace, output_csv=args.output_csv)
    print(f"Done. Result: {len(df)} rows, {len(df.columns)} columns")


def cmd_preprocess_xml(args: argparse.Namespace) -> None:
    """Handle the XML preprocessing subcommand."""
    extractor = _build_extractor(args)
    print(f"Preprocessing XML full text (xml_root={args.xml_root})...")
    df = extractor.step_0_preprocess_xml(xml_root=args.xml_root)
    print(f"Done. Result: {len(df)} rows, {len(df.columns)} columns")


def cmd_fetch_sequences(args: argparse.Namespace) -> None:
    """Handle the 'sequences' subcommand."""
    extractor = _build_extractor(args)
    print(f"Fetching sequences (pdf_root={args.pdf_root})...")
    summary = extractor.step_4_fetch_sequences(
        pdf_root=args.pdf_root,
        pmids_csv=args.pmids_csv,
        entrez_email=args.entrez_email,
        output_dir=args.output_dir,
    )
    print(summary)
    print("Done.")


def cmd_attach_sequences(args: argparse.Namespace) -> None:
    """Handle the 'attach' subcommand."""
    extractor = _build_extractor(args)
    print(f"Attaching sequences (download_csv={args.download_csv})...")
    df = extractor.step_5_attach_sequences(
        download_csv=args.download_csv,
        sequences_dir=args.sequences_dir,
        output_csv=args.output_csv,
        use_llm=args.use_llm,
    )
    print(f"Done. Result: {len(df)} rows, {len(df.columns)} columns")


COMMAND_DISPATCH = {
    "submit": cmd_submit,
    "xml": cmd_preprocess_xml,
    "download": cmd_download_results,
    "sequences": cmd_fetch_sequences,
    "attach": cmd_attach_sequences,
}


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the EnzyExtract CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    handler = COMMAND_DISPATCH.get(args.command)
    if handler is not None:
        handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
