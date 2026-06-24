import polars as pl
from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.pipeline.step5_generate_identifiers import step5_main

@resolve
@export("data/export/TheData.parquet")
@export("data/export/TheData_kcat.parquet")
def main_example5(
    gpt_df = REQUIRE('data/export/TheData_bare.parquet', eager=True)
):
    subs_df = pl.read_parquet('data/thesaurus/substrate/latest_substrate_thesaurus.parquet')

    # At this point, you will need to run the enzyme accession steps first
    # before being able to attach enzyme sequences to the data.
    df = step5_main(
        gpt_df=gpt_df,
        subs_df=subs_df,
        include_enzyme_sequences=True,
    )
    df.write_parquet('data/export/TheData.parquet')

    df_kcat = df.filter(
        pl.col('kcat').is_not_null()
    )
    print("generating data/export/TheData_kcat.parquet")
    df_kcat.write_parquet('data/export/TheData_kcat.parquet')

if __name__ == "__main__":
    main_example5()

    