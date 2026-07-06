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
        from enzyextract.pre.scans.scan_to_parquet import scan_papers
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
        df = scan_papers(
            pdfs_folder=self.pdf_root,
            recursive=False,
        )
        df.write_parquet(f'{self.fm.pdf_scans_dir}/pdf.parquet')

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
