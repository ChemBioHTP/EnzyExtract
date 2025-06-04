import polars as pl


def attach_regurgitation_flag(data: pl.DataFrame, decoded_df: pl.DataFrame, common_key='custom_id'):
    """
    Flag whether values from the prompt have been regurgitated.

    Pre:
    - data: DataFrame with 'pmid' column.
    - decoded_df: should contain 'pmid' and 'content' columns.

    Returns:
    - DataFrame with additional column 'flag.regurgitated'
    
    NOTE: this requires the *decoded_df*, which is the output of json_to_decoded_df().
    TheData_bare.parquet will NOT work!
    """

    prompt_fragment = """
    - descriptor: R190Q cat-1; 25°C
      substrate: H2O2
      kcat: 33 ± 0.3 s^-1
      Km: 2.3 mM
      kcat/Km: null"""
    
    prompt_fragment2 = """    - descriptor: R203Q cat-1; with NADPH; 25°C
      substrate: H2O2
      kcat: null
      Km: 9.9 ± 0.1 µM
      kcat/Km: 4.4 s^-1 mM^-1"""
    

    bad_llm_outputs = decoded_df.filter(
        pl.col('content').str.contains_any([
            prompt_fragment,
            prompt_fragment2
        ]).alias('flag.regurgitated')
    ).select('custom_id')
    
