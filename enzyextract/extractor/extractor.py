from dataclasses import dataclass
from pathlib import Path
import polars as pl
from typing import TYPE_CHECKING, Literal, Optional, Union

from enzyextract.submit.openai_management import process_env
from enzyextract.submit.openai_synch import process_batch_synchronously
from enzyextract.utils.namespace_management import glean_model_name

if TYPE_CHECKING:
    import pandas as pd

@dataclass
class EnzyExtractConfig:
    """
    Configure EnzyExtract settings.
    """


    reocr_model_path: Union[str, Path] = "data/models/resnet18-remicro-iter3.pth"
    """Location of the ResNet18 model for mM OCR (default: data/models/resnet18-remicro-iter3.pth)"""

    llm_name: str = "openai/gpt-4o"
    """Name of the LLM to use. Currently, only OpenAI models are supported."""


class IntermediateFileManager:
    """
    Configure locations of intermediate files.
    """

    def __init__(self, base_dir: Union[str, Path]):
        self.enzy_root = Path(base_dir).as_posix()

        # Step 0
        self.mM_dir = f"{self.enzy_root}/pre/mM"
        """Path to mM OCR results (default: .enzy/pre/mM)"""

        self.tables_dir = f"{self.enzy_root}/pre/tables"
        """Path to tables directory (default: .enzy/pre/tables)"""

        self.pdf_scans_dir = f"{self.enzy_root}/scans/pdf"
        """Path to compressed PDF fulltexts (default: .enzy/scans/pdf)"""

        self.xml_scans_dir = f"{self.enzy_root}/scans/xml"
        """Path to compressed XML fulltexts (default: .enzy/scans/xml)"""

        self.batches_dir = f"{self.enzy_root}/batches"
        """Path to LLM batch results (default: .enzy/batches)"""

        self.corresp_dir = f"{self.enzy_root}/corresp"
        """Path to LLM correspondence files (custom_id mappings) (default: .enzy/corresp)"""

        self.llm_log_tsv = f"{self.enzy_root}/llm_log.tsv"
        """Path to location where LLM batches are stored (default: .enzy/llm_log.tsv)"""

        self.completions_dir = f"{self.enzy_root}/completions"
        """Path to location where LLM completions are stored (default: .enzy/completions)"""

        self.errors_dir = f"{self.enzy_root}/errors"
        """Path to location where LLM errors are stored (default: .enzy/errors)"""

        self.llm_df_dir = f"{self.enzy_root}/post/valid"
        """Path to location where LLM results are stored as a Polars DataFrame (default: .enzy/post/valid)"""

        self.sequences_dir = f"{self.enzy_root}/sequences"
        """Path to location where fetched accession sequences are stored (default: .enzy/sequences)"""

        for folder in [
            self.mM_dir,
            self.tables_dir,
            self.tables_markdown_dir,
            self.pdf_scans_dir,
            self.xml_scans_dir,
            self.batches_dir,
            self.corresp_dir,
            self.completions_dir,
            self.errors_dir,
            self.llm_df_dir,
            self.sequences_dir,
        ]:
            Path(folder).mkdir(parents=True, exist_ok=True)

    @property
    def mM_parquet(self):
        """Path to the mM parquet file (default: .enzy/pre/mM/mM.parquet)"""
        return f"{self.mM_dir}/mM.parquet"

    @property
    def tables_markdown_dir(self):
        """Path to the markdown tables directory (default: .enzy/pre/tables/markdown)"""
        return f"{self.tables_dir}/markdown"


class EnzyExtract:
    def __init__(
        self,
        enzy_root: Union[str, Path],
        pdf_root: Union[str, Path],
        *,
        xml_root: Optional[Union[str, Path]] = None,
        config: Optional[EnzyExtractConfig] = None,
    ):
        """
        Initialize the EnzyExtract instance.
        :param base_dir: Path to folder where extracted data will be stored.
        :param pdf_root: Path to the folder of PDFs to process.
        :param xml_root: Path to the folder of XMLs to process.
        """
        self.enzy_root = Path(enzy_root).as_posix()
        self.pdf_root = Path(pdf_root).as_posix() if pdf_root is not None else None
        """Path to the folder of PDFs to process"""
        self.xml_root = Path(xml_root).as_posix() if xml_root is not None else None
        """Path to the folder of XMLs to process"""
        self.fm = IntermediateFileManager(base_dir=enzy_root)
        self.config = config if config is not None else EnzyExtractConfig()

    def step_0_preprocess_pdf(self):
        """
        Runs preprocessing of PDF documents.
        """        
        from enzyextract.pre.reocr.m_mu_reocr import script_scan_mM
        from enzyextract.pre.table.scan_tables import process_pdfs

        if self.pdf_root is None:
            print("No PDFs specified. Skipping PDF preprocessing.")
            return

        print("Starting mM...")
        script_scan_mM(
            pdf_root=self.pdf_root, 
            write_dir=self.fm.mM_dir, 
            model_path=self.config.reocr_model_path
        )

        print("Starting tables...")
        process_pdfs(
            pdf_root=self.pdf_root,
            write_dir=self.fm.tables_dir,
            micros_path=self.fm.mM_parquet
        )

        print(f"Compressing PDFs to {self.fm.pdf_scans_dir}/pdf.parquet")
        self.scan_papers_and_save(
            pdfs_folder=self.pdf_root,
            recursive=False,
        )

    def step_0_preprocess_xml(self):
        """
        Run preprocessing of XML documents.
        """
        from enzyextract.pre.scans.scan_to_parquet import scan_xmls_by_folder

        if self.xml_root is None:
            print("No XMLs specified. Skipping XML preprocessing.")
            return
        print(f"Compressing XMLs to {self.fm.xml_scans_dir}/xml.parquet")
        df = scan_xmls_by_folder(
            xmls_folder=self.xml_root,
            recursive=False,
        )
        df.write_parquet(f'{self.fm.xml_scans_dir}/xml.parquet')

    def step_1_ask_llm(
        self,
        namespace="default-namespace",
        *,
        version: Optional[str]=None,
        mode: Literal["interactive", "batch", "confirm"]=None
    ):
        """
        Make calls to LLMs in batches, using the preprocessed data from step 0.

        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
        :param version: Optional version, such as "v1", to distinguish new versions of a namespace.
            If not provided, a version will be assigned automatically.
        :param mode: Mode for the LLM batch. Can be "interactive", "batch", or "confirm".

        """
        from enzyextract.pipeline.step1_run_tableboth import step1_main


        process_env('.env')

        llm_provider = 'openai'
        _, suggested_prompt, structured = glean_model_name('baba-standard')
        model_name = self.config.llm_name.removeprefix("openai/")

        if any(
            ch in namespace for ch in [":", "/", "\\", "*", "?", "\"", "<", ">", "|"]
        ):
            raise ValueError(f"Namespace '{namespace}' must be a valid file name (no colons, etc.)")

        confirmation = None
        if mode == "interactive":
            confirmation = "local"
        elif mode == "batch":
            confirmation = "yes"
        step1_main(
            namespace=namespace,
            pdf_root=self.pdf_root,
            micro_path=self.fm.mM_parquet,
            tables_from=self.fm.tables_markdown_dir,
            dest_folder=self.fm.batches_dir,
            corresp_folder=self.fm.corresp_dir,
            log_location=self.fm.llm_log_tsv,
            model_name=model_name,
            llm_provider=llm_provider,
            prompt=suggested_prompt,
            structured=structured,
            version=version,
            confirmation=confirmation,
            # allow for no preprocessing
            _check_nonzero_reocr=False,
            _check_nonzero_tables=False,
        )

    def step_2_process_batch_serially(self, namespace="default-namespace"):
        """
        Execute LLM batches serially, without the Batch API.
        """
        process_env('.env')
        from enzyextract.pipeline.step3_llm_to_df import namespace_with_version

        _, _, row = namespace_with_version(
            namespace=namespace,
            log_location=self.fm.llm_log_tsv,
        )
        batch_fpath = row.item(row=0, column='batch_fpath')
        print("Processing batch file:", batch_fpath)
        process_batch_synchronously(
            batch_fpath=batch_fpath,
            enzy_root=self.enzy_root,
        )

    def step_2_download_batches(self):
        """
        Downloads LLM batches.
        """
        from enzyextract.pipeline.step2_download import download

        process_env('.env')
        download(
            log_location=self.fm.llm_log_tsv,
            dest_folder=self.fm.completions_dir,
            err_folder=self.fm.errors_dir,
        )

    def step_3_llm_to_df(self, namespace="default-namespace") -> "pd.DataFrame":
        """
        Converts LLM batch results to a Pandas DataFrame.

        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
            If this namespace has multiple versions, the latest version will be used.
        """
        from enzyextract.pipeline.step3_llm_to_df import namespace_to_parquet

        df = namespace_to_parquet(
            namespace=namespace,
            log_location=self.fm.llm_log_tsv,
            write_dir=self.fm.llm_df_dir,
        )
        return df

    def submit_pdfs(
        self,
        namespace="default-namespace",
        *,
        version: Optional[str]=None,
        mode: Literal["interactive", "batch", "confirm"]="confirm"
    ):
        """
        Preprocess PDFs and ask the LLM.

        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
        :param version: Optional version, such as "v1", to distinguish new versions of a namespace.
            If not provided, a version will be assigned automatically.
        """

        self.step_0_preprocess_pdf()
        self.step_1_ask_llm(namespace=namespace, version=version, mode=mode)

        if mode == "interactive":
            self.step_2_process_batch_serially(namespace=namespace)


    def download_results(self, namespace="default-namespace", *, output_csv: Optional[str]=None) -> "pl.DataFrame":
        """
        Run the entire EnzyExtract pipeline: preprocess PDFs and XMLs, ask the LLM, and convert results to a DataFrame.

        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
        :param version: Optional version, such as "v1", to distinguish new versions of a namespace.
            If not provided, a version will be assigned automatically.
        """

        self.step_2_download_batches()
        df = self.step_3_llm_to_df(namespace=namespace)

        if output_csv is not None:
            df.to_csv(output_csv)
            print(f"Results written to {output_csv}")
        return df

    def scan_papers_and_save(
        self,
        pdfs_folder=None,
        *,
        recursive=False,
    ):
        """
        Scan PDFs into a DataFrame with progress tracking via checkpoint files.

        :param pdfs_folder: Path to the folder of PDFs to process. If None, uses self.pdf_root.
        :param recursive: Whether to scan PDFs recursively in subdirectories.
        """
        
        parquet_loc = f"{self.fm.pdf_scans_dir}/pdf.parquet"

        if Path(parquet_loc).exists():
            print(f"Found existing parquet file at {parquet_loc}. Loading...")
            df = pl.read_parquet(parquet_loc)
            return df
        else:
            from enzyextract.pre.scans.scan_to_parquet import scan_papers

            df = scan_papers(pdfs_folder=pdfs_folder, recursive=recursive)
            df.write_parquet(f"{self.fm.pdf_scans_dir}/pdf.parquet")
            return df
    # ──────────────────────────────────────────────
    # sequences: scan PDFs for accession IDs & fetch
    # ──────────────────────────────────────────────

    def fetch_sequences(
        self,
        pdf_root: str,
        *,
        pmids_csv: Optional[str] = None,
        output_dir: Optional[str] = None,
    ) -> "pl.DataFrame":
        """
        Scan PDFs for accession IDs (PDB, UniProt, RefSeq, GenBank) and fetch
        their sequences from the respective APIs.

        This wraps the logic of acc1–acc3 from the accessions pipeline:

          1. acc1 — scan PDFs for accession IDs
          2. acc2 — fetch sequences from UniProt / PDB / NCBI
          3. acc3 — (optional) fetch UniProt entries linked to a list of PMIDs

        Parameters
        ----------
        pdf_root : str
            Directory containing PDF files to scan.
        pmids_csv : str, optional
            Path to a CSV with a ``pmid`` column.  If given, UniProt entries
            associated with those PMIDs will also be fetched.
        output_dir : str, optional
            Where to write the intermediate parquet files.  Defaults to
            ``{enzy_root}/sequences``.

        Returns
        -------
        pl.DataFrame
            A DataFrame with columns described below.
        """
        from enzyextract.pre.scans.scan_accessions import extract_enzyme_accessions
        from enzyextract.fetch_sequences.query_idents import fetch_pdbs, fetch_ncbis
        from enzyextract.fetch_sequences.query_uniprot import (
            fetch_uniprots_latest,
            fetch_uniprots_from_pmids,
        )

        if output_dir is None:
            output_dir = self.fm.sequences_dir

        Path(output_dir).mkdir(parents=True, exist_ok=True)
        print(f"[sequences] Output directory: {output_dir}")

        # ---- 1. Scan PDFs & extract accession IDs --------------------------
        # (uses the better regexes from scan_accessions.py / protein_patterns.py)
        print(f"[sequences] Scanning PDFs in {pdf_root} …")
        scan_df = self.scan_papers_and_save(
            pdfs_folder=pdf_root,
            recursive=False,
        )
        print(f"[sequences] Scanned {scan_df.height} page(s) from PDF(s)")

        print("[sequences] Extracting accession IDs from text …")
        idents_lf = extract_enzyme_accessions(scan_df.lazy(), col=pl.col("text"))
        idents_df = idents_lf.collect()
        print(f"[sequences] Extracted accessions from {idents_df.height} page(s)")

        # Write checkpoint — native polars List(Utf8) columns write cleanly
        idents_df.write_parquet(f"{output_dir}/scanned_accessions.parquet")
        print(f"[sequences] Saved scanned_accessions.parquet ({idents_df.height} rows)")

        # Collect unique IDs per type (polars-native: explode lists → unique)
        def _unique_ids(col_name: str) -> set[str]:
            if col_name not in idents_df.columns:
                return set()
            col = idents_df[col_name]
            if col.dtype == pl.Null:
                return set()
            return set(col.drop_nulls().explode().unique().to_list())

        all_pdb = _unique_ids("pdb")
        all_uniprot = _unique_ids("uniprot")
        all_refseq = _unique_ids("refseq")
        all_genbank = _unique_ids("genbank")

        print(
            f"[sequences] Found {len(all_pdb)} PDB, {len(all_uniprot)} UniProt, "
            f"{len(all_refseq)} RefSeq, {len(all_genbank)} GenBank ID(s)"
        )

        # ---- 2. Fetch PDB sequences ----------------------------------------
        pdb_df = pl.DataFrame()
        if all_pdb:
            print(f"[sequences] Fetching {len(all_pdb)} PDB entries …")
            pdb_df = pl.from_pandas(fetch_pdbs(list(all_pdb)))
            pdb_df.write_parquet(f"{output_dir}/pdb_sequences.parquet")
            print(f"[sequences] Got {len(pdb_df)} PDB rows")

        # ---- 3. Fetch UniProt sequences ------------------------------------
        uniprot_df = pl.DataFrame()
        if all_uniprot:
            print(f"[sequences] Fetching {len(all_uniprot)} UniProt entries …")
            uniprot_df = fetch_uniprots_latest(list(all_uniprot))
            uniprot_df.write_parquet(f"{output_dir}/uniprot_sequences.parquet")
            print(f"[sequences] Got {len(uniprot_df)} UniProt rows")

        # ---- 4. Fetch NCBI sequences (RefSeq + GenBank) --------------------
        ncbi_df = pl.DataFrame()
        all_ncbi = list(all_refseq | all_genbank)
        if all_ncbi:
            print(f"[sequences] Fetching {len(all_ncbi)} NCBI entries …")
            ncbi_df = pl.from_pandas(fetch_ncbis(all_ncbi))
            ncbi_df.write_parquet(f"{output_dir}/ncbi_sequences.parquet")
            print(f"[sequences] Got {len(ncbi_df)} NCBI rows")

        # ---- 5. (Optional) Fetch UniProt entries linked to PMIDs -----------
        pmid_uniprot_df = pl.DataFrame()
        if pmids_csv:
            pmids_df = pl.read_csv(pmids_csv)
            if "pmid" not in pmids_df.columns:
                print("[sequences] WARNING: pmids_csv has no 'pmid' column — skipping")
            else:
                pmids = pmids_df["pmid"].drop_nulls().unique().to_list()
                print(f"[sequences] Fetching UniProt entries for {len(pmids)} PMID(s) …")
                pmid_uniprot_df = fetch_uniprots_from_pmids(pmids)
                pmid_uniprot_df.write_parquet(
                    f"{output_dir}/pmid_uniprot_sequences.parquet"
                )
                print(f"[sequences] Got {len(pmid_uniprot_df)} PMID-linked UniProt rows")

        # ---- 6. Return summary ---------------------------------------------
        n_unique_pdfs = scan_df["pmid"].n_unique()
        summary = pl.DataFrame(
            {
                "source": ["scanned_pdfs", "with_accessions", "pdb", "uniprot", "ncbi", "pmid_uniprot"],
                "count": [
                    n_unique_pdfs,
                    idents_df["pmid"].n_unique(),
                    len(pdb_df),
                    len(uniprot_df),
                    len(ncbi_df),
                    len(pmid_uniprot_df),
                ],
            }
        )
        print("[sequences] Done.")
        return summary

    # ──────────────────────────────────────────────
    # attach: match accessions to GPT data & output
    # ──────────────────────────────────────────────

    def attach_sequences(
        self,
        download_csv: str,
        sequences_dir: str,
        *,
        output_csv: Optional[str] = None,
        use_llm: bool = False,
    ) -> "pl.DataFrame":
        """
        Attach enzyme sequences to the GPT-extracted data by matching accession
        descriptions to enzyme names via string similarity (and optionally LLM).

        This corresponds to the 'attach' subcommand and wraps the core matching
        logic found in ``step5_generate_identifiers``.

        Parameters
        ----------
        download_csv : str
            Path to the CSV produced by the ``download`` subcommand (the
            GPT-extracted enzyme kinetic data).
        sequences_dir : str
            Directory containing the parquet files written by
            :meth:`fetch_sequences` (``pdb_sequences.parquet``,
            ``uniprot_sequences.parquet``, ``ncbi_sequences.parquet``).
        output_csv : str, optional
            If provided, the final enriched DataFrame is written to this CSV.
        use_llm : bool
            Whether to use an LLM to confirm/improve accession matching
            (default: ``False``).

        Returns
        -------
        pl.DataFrame
            The GPT-extracted DataFrame augmented with sequence columns.
        """
        import os

        from rapidfuzz import fuzz

        from enzyextract.fetch_sequences.accession_schemas import (
            pdb_df_schema,
            uniprot_df_schema,
        )

        # ---- 1. Load GPT-extracted data ------------------------------------
        print(f"[attach] Loading download CSV: {download_csv}")
        gpt_df = pl.read_csv(download_csv)
        print(f"[attach] Loaded {len(gpt_df)} rows with columns: {gpt_df.columns}")

        # ---- 2. Load fetched sequences -------------------------------------
        pdb_path = os.path.join(sequences_dir, "pdb_sequences.parquet")
        uniprot_path = os.path.join(sequences_dir, "uniprot_sequences.parquet")
        ncbi_path = os.path.join(sequences_dir, "ncbi_sequences.parquet")

        pdb_df = pl.read_parquet(pdb_path) if os.path.exists(pdb_path) else pl.DataFrame()
        uniprot_df = (
            pl.read_parquet(uniprot_path) if os.path.exists(uniprot_path) else pl.DataFrame()
        )
        ncbi_df = (
            pl.read_parquet(ncbi_path) if os.path.exists(ncbi_path) else pl.DataFrame()
        )
        print(
            f"[attach] Loaded {len(pdb_df)} PDB, {len(uniprot_df)} UniProt, "
            f"{len(ncbi_df)} NCBI sequences"
        )

        # ---- 3. Ensure required columns exist on GPT df --------------------
        for col in ("sequence", "sequence_source", "uniprot", "ncbi", "pdb"):
            if col not in gpt_df.columns:
                gpt_df = gpt_df.with_columns(pl.lit(None).alias(col))

        # ---- 4. Build a unified accession lookup ---------------------------
        # We collect every accession along with its descriptive text and
        # sequence so we can fuzzy-match against enzyme names.

        accession_records: list[dict] = []

        # PDB
        if pdb_df.height > 0:
            for row in pdb_df.iter_rows(named=True):
                desc = (
                    row.get("name")
                    or row.get("sys_name")
                    or row.get("descriptor")
                    or ""
                )
                accession_records.append(
                    {
                        "source": "pdb",
                        "accession": str(row.get("pdb", "")),
                        "description": str(desc),
                        "organism": str(row.get("organism") or ""),
                        "sequence": str(row.get("seq_can") or row.get("seq") or ""),
                    }
                )

        # UniProt
        if uniprot_df.height > 0:
            for row in uniprot_df.iter_rows(named=True):
                accession_records.append(
                    {
                        "source": "uniprot",
                        "accession": str(row.get("uniprot", "")),
                        "description": str(row.get("enzyme_name") or ""),
                        "organism": str(row.get("organism") or ""),
                        "sequence": str(row.get("sequence") or ""),
                    }
                )

        # NCBI
        if ncbi_df.height > 0:
            for row in ncbi_df.iter_rows(named=True):
                accession_records.append(
                    {
                        "source": "ncbi",
                        "accession": str(row.get("ncbi", "")),
                        "description": str(row.get("descriptor") or ""),
                        "organism": "",
                        "sequence": str(row.get("sequence") or ""),
                    }
                )

        if not accession_records:
            print("[attach] WARNING: no accession records loaded — nothing to match")
            if output_csv:
                gpt_df.write_csv(output_csv)
            return gpt_df

        acc_df = pl.DataFrame(accession_records)
        print(f"[attach] Built lookup table with {len(acc_df)} accession record(s)")

        # ---- 5. Fuzzy-match each GPT row to the best accession -------------
        # We match on (enzyme + organism) against (description + organism).

        match_results: list[dict] = []

        for row in gpt_df.iter_rows(named=True):
            enzyme = str(row.get("enzyme") or "")
            organism = str(row.get("organism") or "")
            enzyme_full = str(row.get("enzyme_full") or "")

            # Build query strings
            query_enzyme = (enzyme_full or enzyme).lower().strip()
            query_organism = organism.lower().strip()

            best = {
                "accession": None,
                "source": None,
                "sequence": None,
                "score_enzyme": 0.0,
                "score_organism": 0.0,
                "score_total": 0.0,
            }

            for acc_row in acc_df.iter_rows(named=True):
                acc_desc = (acc_row["description"] or "").lower().strip()
                acc_org = (acc_row["organism"] or "").lower().strip()

                if not query_enzyme or not acc_desc:
                    continue

                # String similarity on enzyme name ↔ description
                score_enzyme = fuzz.partial_ratio(query_enzyme, acc_desc)

                # Organism bonus
                score_organism = 0.0
                if query_organism and acc_org:
                    score_organism = fuzz.ratio(query_organism, acc_org)
                elif not query_organism and not acc_org:
                    score_organism = 50.0  # neutral when both missing
                # Penalize when one has organism and the other doesn't
                elif query_organism and not acc_org:
                    score_organism = 25.0
                else:
                    score_organism = 25.0

                score_total = score_enzyme * 0.7 + score_organism * 0.3

                if score_total > best["score_total"] and score_enzyme >= 50:
                    best.update(
                        {
                            "accession": acc_row["accession"],
                            "source": acc_row["source"],
                            "sequence": acc_row["sequence"],
                            "score_enzyme": score_enzyme,
                            "score_organism": score_organism,
                            "score_total": score_total,
                        }
                    )

            match_results.append(
                {
                    "_row_idx": len(match_results),
                    "matched_accession": best["accession"],
                    "matched_source": best["source"],
                    "matched_sequence": best["sequence"],
                    "match_score_enzyme": best["score_enzyme"],
                    "match_score_organism": best["score_organism"],
                    "match_score_total": best["score_total"],
                }
            )

        match_df = pl.DataFrame(match_results)

        # ---- 6. Attach matches back to GPT df ------------------------------
        gpt_df = gpt_df.with_row_index("_row_idx").join(
            match_df.select(
                [
                    "_row_idx",
                    "matched_accession",
                    "matched_source",
                    "matched_sequence",
                    "match_score_enzyme",
                    "match_score_organism",
                    "match_score_total",
                ]
            ),
            on="_row_idx",
            how="left",
        )

        # Coalesce: prefer fuzzy-matched values over None
        gpt_df = gpt_df.with_columns(
            [
                pl.coalesce(["matched_accession", pl.col("uniprot")]).alias("uniprot"),
                pl.coalesce(["matched_sequence", pl.col("sequence")]).alias("sequence"),
                pl.when(pl.col("matched_sequence").is_not_null())
                .then(pl.lit("fuzzy matched"))
                .otherwise(pl.col("sequence_source"))
                .alias("sequence_source"),
            ]
        ).drop("matched_accession", "matched_sequence", "_row_idx")

        # ---- 7. Optional LLM-based refinement ----------------------------
        if use_llm:
            print("[attach] LLM-based matching not yet implemented — skipping")

        # ---- 8. Write output -----------------------------------------------
        if output_csv:
            gpt_df.write_csv(output_csv)
            print(f"[attach] Wrote {len(gpt_df)} rows to {output_csv}")

        print("[attach] Done.")
        return gpt_df


if __name__ == "__main__":
    from enzyextract.extractor.extractor import EnzyExtract, EnzyExtractConfig

    config = EnzyExtractConfig()

    extractor = EnzyExtract(
        enzy_root=".enzy",
        pdf_root="pdfs",
        config=config,
    )
    print("OK")
    extractor.submit_pdfs()

    extractor.download_results()
