from pathlib import Path
from typing import List, Union

import polars as pl
from tqdm import tqdm

from enzyextract.dependency.prereqs import export
from enzyextract.utils.doi_management import doi_to_filename


def _example_manifest():
    # this is an example manifest.
    df = pl.DataFrame({
        "fileroot": ["D:/papers/wos/wiley"],
        "filename": ["10.1002_1873-3468.12165.pdf"],
        "pmid": ["10.1002_1873-3468.12165"],
        "canonical": ["27062179"],
        "canonical_filename": ["27062179.pdf"],
        "readable": [True],
        "toplevel": ["wos"],
        "secondlevel": ["wiley"],
    })
    return df

@export("data/manifest.parquet")
def create_manifest(
    pdf_folders: List[Union[str, Path]],
    filename_to_canonical: pl.DataFrame,
    *,
    check_if_readable: bool = False,
) -> pl.DataFrame:
    """
    Takes a list of folders containing PDFs
    also takes a dataframe mapping filenames to canonical PMIDs

    Then generates a manifest.

    WARNING: In EnzyExtract, "pmid" is a major misnomer - it is a unique identifier for papers,
    which is usually but not always a PubMed ID (can also be a modified DOI.)
    """
    collector = []
    for pdf_folder in pdf_folders:
        pdf_folder = Path(pdf_folder)
        if not pdf_folder.exists():
            raise FileNotFoundError(f"PDF folder {pdf_folder} does not exist.")

        for pdf_path in tqdm(pdf_folder.rglob("*.pdf")):
            filename = pdf_path.name
            fileroot = str(pdf_path.parent)

            readable = True
            if check_if_readable:
                import pymupdf
                try:
                    pdf = pymupdf.open(str(pdf_path))
                    pdf.close()
                except:
                    readable = False

            toplevel = pdf_folder.name
            secondlevel = pdf_path.parent.name

            collector.append({
                "fileroot": fileroot,
                "filename": filename,
                "readable": readable,
                "toplevel": toplevel,
                "secondlevel": secondlevel,
            })
    
    df = pl.DataFrame(collector)

    assert "filename" in filename_to_canonical.columns, "filename_to_canonical needs a 'filename' column."
    assert "canonical" in filename_to_canonical.columns, "filename_to_canonical needs a 'canonical' column."

    if "pmid" in filename_to_canonical.columns:
        transform_view = filename_to_canonical.select(
            "filename", "pmid", "canonical"
        )
    else:
        transform_view = filename_to_canonical.select(
            "filename", "canonical"
        ).with_columns(
            pl.col("filename").str.strip_suffix(".pdf").alias("pmid")
        )
    df = df.join(transform_view.unique(
        "filename",
        keep="first",
        maintain_order=True
    ), left_on="filename", right_on="filename", how="left")
    df = df.select(
        "fileroot",
        "filename",
        "pmid",
        "canonical",
        pl.col("canonical").map_elements(
            lambda x: doi_to_filename(x, '.pdf'), return_dtype=pl.Utf8
        ).alias("canonical_filename"),
        "readable",
        "toplevel",
        "secondlevel",
    )
    return df

if __name__ == "__main__":
    df = create_manifest(
        pdf_folders=[
            "D:/papers/wos",
        ],
        filename_to_canonical=_example_manifest().select("filename", "pmid", "canonical"),
        check_if_readable=False,
    )
    df.write_parquet("data/manifest-preview.parquet")
    print("Wrote preview manifest to data/manifest-preview.parquet")
    pass