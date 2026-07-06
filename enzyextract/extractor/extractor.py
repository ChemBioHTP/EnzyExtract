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
        *,
        config: Optional[EnzyExtractConfig] = None,
    ):
        """
        Initialize the EnzyExtract instance.
        :param base_dir: Path to folder where extracted data will be stored.
        """
        self.enzy_root = Path(enzy_root).as_posix()
        self.fm = IntermediateFileManager(base_dir=enzy_root)
        self.config = config if config is not None else EnzyExtractConfig()

    def step_0_preprocess_pdf(self, pdf_root):
        """
        Runs preprocessing of PDF documents.

        :param pdf_root: Path to the folder of PDFs to process.
        """        
        from enzyextract.pre.reocr.m_mu_reocr import script_scan_mM
        from enzyextract.pre.table.scan_tables import process_pdfs

        print("Starting mM...")
        script_scan_mM(
            pdf_root=pdf_root, 
            write_dir=self.fm.mM_dir, 
            model_path=self.config.reocr_model_path
        )

        print("Starting tables...")
        process_pdfs(
            pdf_root=pdf_root,
            write_dir=self.fm.tables_dir,
            micros_path=self.fm.mM_parquet
        )

        self.scan_papers_and_save(
            pdfs_folder=pdf_root,
            recursive=False,
        )

    def step_0_preprocess_xml(self, xml_root):
        """
        Run preprocessing of XML documents.

        :param xml_root: Path to the folder of XMLs to process.
        """
        from enzyextract.pre.scans.scan_to_parquet import scan_xmls_by_folder

        print(f"Compressing XMLs to {self.fm.xml_scans_dir}/xml.parquet")
        df = scan_xmls_by_folder(
            xmls_folder=xml_root,
            recursive=False,
        )
        df.write_parquet(f'{self.fm.xml_scans_dir}/xml.parquet')

    def step_1_ask_llm(
        self,
        namespace="default-namespace",
        *,
        pdf_root,
        version: Optional[str]=None,
        mode: Literal["interactive", "batch", "confirm"]=None
    ):
        """
        Make calls to LLMs in batches, using the preprocessed data from step 0.

        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
        :param pdf_root: Path to the folder of PDFs to process.
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
        return step1_main(
            namespace=namespace,
            pdf_root=pdf_root,
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
        pdf_root,
        namespace="default-namespace",
        *,
        version: Optional[str]=None,
        mode: Literal["interactive", "batch", "confirm"]="confirm"
    ):
        """
        Preprocess PDFs and ask the LLM.

        :param pdf_root: Path to the folder of PDFs to process.
        :param namespace: Namespace for the LLM batch. Must be a valid file name (no colons, etc.).
        :param version: Optional version, such as "v1", to distinguish new versions of a namespace.
            If not provided, a version will be assigned automatically.
        """

        self.step_0_preprocess_pdf(pdf_root=pdf_root)
        status = self.step_1_ask_llm(namespace=namespace, pdf_root=pdf_root, version=version, mode=mode)

        if mode == "interactive" and status:
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

        :param pdfs_folder: Path to the folder of PDFs to process.
        :param recursive: Whether to scan PDFs recursively in subdirectories.
        """
        
        parquet_loc = f"{self.fm.pdf_scans_dir}/pdf.parquet"

        if Path(parquet_loc).exists():
            print(f"Found existing parquet file at {parquet_loc}.")
            df = pl.read_parquet(parquet_loc)
            return df
        else:
            from enzyextract.pre.scans.scan_to_parquet import scan_papers

            print(f"Compressing PDFs to {self.fm.pdf_scans_dir}/pdf.parquet")
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
        subs_df: Optional["pl.DataFrame"] = None,
    ) -> "pl.DataFrame":
        """
        Attach enzyme sequences to the GPT-extracted data.

        This method now delegates to the proper ``step5_generate_identifiers``
        pipeline (``add_identifiers`` + ``add_enzyme_sequences``) instead of
        using an ad-hoc fuzzy-matching routine.  The old fuzzy-matching logic
        has been moved to
        ``enzyextract.pipeline.accessions.shortcut_pick_accessions``.

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
            (default: ``False``).  Currently a no-op.
        subs_df : pl.DataFrame, optional
            Substrate thesaurus DataFrame (columns ``name``, ``cid``,
            ``brenda_id``, ``smiles``, ``smiles_brenda``).  If not provided,
            an empty DataFrame is used, so identifier columns (EC, CID, …)
            will be left as null.

        Returns
        -------
        pl.DataFrame
            The GPT-extracted DataFrame augmented with sequence and
            identifier columns.
        """
        import os

        from enzyextract.pipeline.step5_generate_identifiers import (
            add_enzyme_sequences,
        )
        from enzyextract.pipeline.accessions.shortcut_pick_accessions import (
            pick_accessions_by_fuzzy_match,
        )

        # ---- 1. Load GPT-extracted data ------------------------------------
        print(f"[attach] Loading download CSV: {download_csv}")
        gpt_df = pl.read_csv(download_csv, schema_overrides={"pmid": pl.Utf8})
        print(f"[attach] Loaded {len(gpt_df)} rows with columns: {gpt_df.columns}")

        # Ensure ``canonical`` exists (needed by add_enzyme_sequences).
        if "canonical" not in gpt_df.columns:
            gpt_df = gpt_df.with_columns(pl.col("pmid").alias("canonical"))

        # ---- 3. Load fetched sequences -------------------------------------
        pdb_path = f"{sequences_dir}/pdb_sequences.parquet"
        uniprot_path = f"{sequences_dir}/uniprot_sequences.parquet"
        ncbi_path = f"{sequences_dir}/ncbi_sequences.parquet"

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

        # ---- 4. Fuzzy-match accessions via shortcut_pick_accessions ---------
        result = pick_accessions_by_fuzzy_match(
            gpt_df=gpt_df,
            pdb_df=pdb_df,
            uniprot_df=uniprot_df,
            ncbi_df=ncbi_df,
        )

        # ---- 5. Add enzyme sequences via step5 pipeline --------------------
        # Empty confidence/alignment DataFrames with the correct schemas.
        _conf_schema = {
            "canonical": pl.Utf8,
            "enzyme": pl.Utf8,
            "enzyme_full": pl.Utf8,
            "organism": pl.Utf8,
            "max_enzyme_similarity": pl.Float64,
            "max_organism_similarity": pl.Float64,
            "total_similarity": pl.Float64,
            "sequence": pl.Utf8,
        }

        uniprot_conf = pl.DataFrame(
            schema={**_conf_schema, "uniprot": pl.Utf8}
        )
        pdb_conf = pl.DataFrame(
            schema={**_conf_schema, "pdb": pl.Utf8}
        )
        ncbi_conf = pl.DataFrame(
            schema={**_conf_schema, "ncbi": pl.Utf8}
        )

        uniprot_searched = pl.DataFrame(
            schema={
                "query_enzyme": pl.Utf8,
                "query_organism": pl.Utf8,
                "uniprot": pl.Utf8,
                "sequence": pl.Utf8,
                "max_enzyme_similarity": pl.Float64,
                "max_organism_similarity": pl.Float64,
            }
        )

        gpt_df = add_enzyme_sequences(
            gpt_df,
            uniprot_conf=uniprot_conf,
            pdb_conf=pdb_conf,
            ncbi_conf=ncbi_conf,
            uniprot2seq=result["uniprot2seq"],
            pdb2seq=result["pdb2seq"],
            ncbi2seq=result["ncbi2seq"],
            uniprot_picked=result["uniprot_picked"].unique(keep="first"),
            pdb_picked=result["pdb_picked"].unique(keep="first"),
            ncbi_picked=result["ncbi_picked"].unique(keep="first"),
            uniprot_cited=None,
            uniprot_searched=uniprot_searched,
        )
        print(f"[attach] After add_enzyme_sequences: {len(gpt_df)} rows")

        # ---- 6. Optional LLM-based refinement ------------------------------
        if use_llm:
            print("[attach] LLM-based matching not yet implemented — skipping")

        # ---- 7. Write output -----------------------------------------------
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
        config=config,
    )
    print("OK")
    extractor.submit_pdfs(pdf_root="pdfs")

    extractor.download_results()
