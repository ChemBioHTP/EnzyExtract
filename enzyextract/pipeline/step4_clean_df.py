import polars as pl

from enzyextract.dependency.injection import OPTIONAL, REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.post.finale.deduplication import deduplicate
from enzyextract.post.finale.hallucination import attach_hallucination_flag
from enzyextract.post.finale.repetition import attach_repetitive_flag, highly_duplicated
from enzyextract.post.metadata.doctype import attach_doctype_meta, attach_doctype_meta_by_custom_id, reattach_custom_id


@resolve
@export("data/export/final/TheData_kcat_best.parquet")
def script_finalize_df(
    thedata_df: pl.DataFrame = REQUIRE('data/export/TheData_bare.parquet'),
    pdf_mask=None,
    _custom_id_source: pl.DataFrame = None,
):
    """

    - pdf_mask: mask that indicates which documents are PDFs/XMLs. There are two options:
        - (option 1) DataFrame with a boolean mask with True for PDFs and False for XMLs.
        - (option 2) None. meta.doctype is deduced from magic strings from the custom_id.
    """
    df = deduplicate(thedata_df)

    if 'custom_id' not in df.columns:
        df = reattach_custom_id(df, _custom_id_source)
    df = attach_doctype_meta(df, pdf_mask)

    df = attach_repetitive_flag(df, threshold=0.35)
    df = attach_hallucination_flag(df, threshold=0.35)
    # conventional_kcat_df = count_kcat_conventionally(deduplicated_df)
    # print(conventional_kcat_df.height)
    pass

if __name__ == "__main__":
    script_finalize_df(_custom_id_source=pl.read_parquet('data/recontext/1_fromyaml/data.parquet'))