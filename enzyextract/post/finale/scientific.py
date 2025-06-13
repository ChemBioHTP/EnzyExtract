import polars as pl

def attach_scientific_flag(gpt_df: pl.DataFrame) -> pl.DataFrame:
    gpt_df = gpt_df.with_columns(
        ((
            pl.col('kcat').str.contains('10\^')
        ) | (
            pl.col('km').str.contains('10\^')
        )).replace(False, None).alias('flag.scientific')
    )
    return gpt_df


