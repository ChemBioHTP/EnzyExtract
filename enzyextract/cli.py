"""
Command-line interface for EnzyExtract.

Usage:
    enzyextract submit     Preprocess PDFs and submit LLM batch
    enzyextract download  Download batch results and convert to DataFrame
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
        "--reocr-model-path",
        default="data/models/resnet18-remicro-iter3.pth",
        help="Path to the ResNet18 mM OCR model (default: data/models/resnet18-remicro-iter3.pth)",
    )
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
        "--pdf-root",
        default=None,
        help="Directory containing PDF files to process",
    )
    parser.add_argument(
        "--xml-root",
        default=None,
        help="Directory containing XML files to process",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # --- submit ---
    submit_parser = subparsers.add_parser(
        "submit",
        help="Preprocess PDFs and create LLM batch files",
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

    return parser


def _build_extractor(args: argparse.Namespace) -> EnzyExtract:
    """Build an EnzyExtract instance from parsed CLI arguments."""
    config = EnzyExtractConfig(
        reocr_model_path=args.reocr_model_path,
        llm_name=args.llm_name,
    )
    extractor = EnzyExtract(
        enzy_root=args.enzy_root,
        pdf_root=args.pdf_root,
        xml_root=args.xml_root,
        config=config,
    )
    return extractor


def cmd_submit_pdfs(args: argparse.Namespace) -> None:
    """Handle the 'submit' subcommand."""
    if not args.pdf_root:
        print("Error: --pdf-root is required for 'submit'.", file=sys.stderr)
        sys.exit(1)

    extractor = _build_extractor(args)
    print(f"Submitting PDFs (namespace={args.namespace}, mode={args.mode})...")
    extractor.submit_pdfs(namespace=args.namespace, version=args.version, mode=args.mode)
    print("Done.")


def cmd_download_results(args: argparse.Namespace) -> None:
    """Handle the 'download' subcommand."""
    extractor = _build_extractor(args)
    print(f"Downloading results (namespace={args.namespace})...")
    df = extractor.download_results(namespace=args.namespace, output_csv=args.output_csv)
    print(f"Done. Result: {len(df)} rows, {len(df.columns)} columns")


COMMAND_DISPATCH = {
    "submit": cmd_submit_pdfs,
    "download": cmd_download_results,
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
