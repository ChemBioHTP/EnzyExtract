import polars as pl

from enzyextract.dependency.injection import OPTIONAL, REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.post.finale.deduplication import deduplicate, deduplicate_with_custom_id
from enzyextract.post.finale.hallucination import attach_hallucination_flag
from enzyextract.post.finale.repetition import attach_repetitive_flag, highly_duplicated
from enzyextract.post.finale.scientific import attach_scientific_flag
from enzyextract.post.metadata.doctype import attach_doctype_meta, attach_doctype_meta_by_custom_id, reattach_custom_id


@resolve
@export("data/export/TheData_pruned.parquet")
def script_finalize_df(
    # thedata_df: pl.DataFrame = REQUIRE('data/export/TheData_bare.parquet'),
    thedata_df: pl.DataFrame = REQUIRE('data/export/TheData.parquet'),
    pdf_mask=None,
    _custom_id_source: pl.DataFrame = None,
    dry_run = False,
):
    """

    - pdf_mask: mask that indicates which documents are PDFs/XMLs. There are two options:
        - (option 1) DataFrame with a boolean mask with True for PDFs and False for XMLs.
        - (option 2) None. meta.doctype is deduced from magic strings from the custom_id.
    """
    # df = deduplicate(thedata_df)

    df = thedata_df

    df = df.unique(maintain_order=True)

    if 'custom_id' not in df.columns:
        df = reattach_custom_id(df, _custom_id_source)
    
    df = deduplicate_with_custom_id(df, unique_column='canonical')
    df = attach_doctype_meta(df, pdf_mask)

    df = attach_repetitive_flag(df, threshold=0.35)

    # This uses hardcoded PDF/XML scans. You would probably want to explicitly pass
    # the scans (dataframe with document full text) to `scan_df`.
    df = attach_hallucination_flag(df, scan_df=None, threshold=0.35)

    df = attach_scientific_flag(df)

    df = df.select(
        pl.col('pmid'),
       pl.selectors.exclude('pmid', 'custom_id')
    )

    # conventional_kcat_df = count_kcat_conventionally(deduplicated_df)
    # print(conventional_kcat_df.height)
    if not dry_run:
        df.write_parquet('data/export/TheData_unpruned.parquet')


    pruned = df.filter(
        pl.col('flag.hallucination').is_null() &
        pl.col('flag.repetitive').is_null() &
        pl.col('flag.scientific').is_null()
    )
    if not dry_run:
        pruned.write_parquet('data/export/TheData_pruned.parquet')

    kcat_only = pruned.filter(
        pl.col('kcat').is_not_null()
    )
    print(kcat_only)
    if not dry_run:
        kcat_only.write_parquet('data/export/EnzyExtractDB_176463.parquet')
    pass

if __name__ == "__main__":
    script_finalize_df(_custom_id_source=pl.read_parquet('data/recontext/1_fromyaml/data.parquet'), dry_run=True)