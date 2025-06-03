import polars as pl
from enzyextract.dependency.base import DependencyNotFoundError
from enzyextract.dependency.prereqs import export
from enzyextract.pipeline.step5_generate_identifiers import step4_main

@export("data/export/TheData_bare.parquet")
@export("data/export/TheData.parquet")
@export("data/export/TheData_kcat.parquet")
def main():
    # raise NotImplementedError("This script is only an example.")
    # gpt_df = pl.read_parquet('data/gpt/apogee_gpt.parquet')
    # gpt_df = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
    # ec_diversity()
    # cid_diversity()
    # exit(0)

    

    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet')

    subs_df = pl.read_parquet('data/thesaurus/substrate/latest_substrate_thesaurus.parquet')

    df_bare = step4_main(
        gpt_df=gpt_df,
        subs_df=subs_df,
        include_enzyme_sequences=False,
    ).filter(
        pl.col('kcat').is_not_null()
    )
    df_bare.write_parquet('data/export/TheData_bare.parquet')

    # At this point, you will need to run the enzyme accession steps first
    # before attaching enzyme sequences to the data.
    try:
        df = step4_main(
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
    except DependencyNotFoundError as e:
        raise RuntimeError(
            "You need to run the enzyme accession steps before being able to generate TheData.parquet."
        ) from e

if __name__ == "__main__":
    main()

    