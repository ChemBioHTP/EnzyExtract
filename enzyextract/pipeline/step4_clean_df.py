import polars as pl

from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.post.finale.deduplication import deduplicate
from enzyextract.post.finale.hallucination import attach_hallucination_flag
from enzyextract.post.finale.repetition import attach_repetitive_flag, highly_duplicated
from enzyextract.post.metadata.doctype import attach_doctype_meta


@resolve
@export("data/export/final/TheData_kcat_best.parquet")
def script_finalize_df(
    thedata_df: pl.DataFrame = REQUIRE('data/export/TheData_bare.parquet'),
    custom_ids: pl.DataFrame = REQUIRE('data/recontext/1_fromyaml/data.parquet')
):
    """
    """
    df = deduplicate(thedata_df)

    df = attach_doctype_meta(df, custom_ids)

    df = attach_repetitive_flag(df, threshold=0.35)

    df = attach_hallucination_flag(df, threshold=0.35)
    # conventional_kcat_df = count_kcat_conventionally(deduplicated_df)
    # print(conventional_kcat_df.height)
    pass

if __name__ == "__main__":
    script_finalize_df()