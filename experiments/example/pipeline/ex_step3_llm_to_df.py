import polars as pl
import os

from enzyextract.dependency.prereqs import export
from enzyextract.pipeline.llm_log import llm_log_schema, read_log
from enzyextract.pipeline.step3_llm_to_df import generate_valid_parquet, namespace_to_parquet


def main_example3():
    namespace_to_parquet(
        namespace='bench dev2',
        log_location='.enzy/llm_log.tsv',
        write_dir='.enzy/post/valid'
    )


@export("data/export/TheData_bare.parquet")
def bare_example3():
    """
    TheData_bare.parquet is created by vertically stacking the results of many runs.
    """
    pass
    

if __name__ == "__main__":
    main_example3()