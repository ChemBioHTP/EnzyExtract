import polars as pl
from enzyextract.pipeline.step4_generate_identifiers import step4_main
if __name__ == "__main__":
    # raise NotImplementedError("This script is only an example.")
    # gpt_df = pl.read_parquet('data/gpt/apogee_gpt.parquet')
    # gpt_df = pl.read_parquet('data/valid/_valid_apogee-rebuilt.parquet')
    # ec_diversity()
    # cid_diversity()
    # exit(0)

    

    gpt_df = pl.read_parquet('data/valid/_valid_everything.parquet')

    subs_df = pl.read_parquet('data/thesaurus/substrate/latest_substrate_thesaurus.parquet')

    df = step4_main(
        gpt_df=gpt_df,
        subs_df=subs_df,
        include_enzyme_sequences=True,
    )
    df.write_parquet('data/export/TheData.parquet')

    df = df.filter(
        pl.col('kcat').is_not_null()
    )
    # 242115
    print("generating data/export/TheData_kcat.parquet")
    df.write_parquet('data/export/TheData_kcat.parquet')

    