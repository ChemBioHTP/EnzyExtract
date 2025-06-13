import polars as pl

def duplicated_run_mask(df: pl.DataFrame, col='pmid') -> pl.Series:
    """
    Create a mask based on duplicated runs. 
    That is, if a pmid is part of a run it is not considered a duplicate, but if the pmid is 
    reintroduced after that initial run, then it is a duplicate.
    """

    seen = set()
    last_element = None
    def is_duplicated(x):
        nonlocal seen, last_element
        if x in seen:
            return True
        elif x == last_element:
            return False
        else:
            seen.add(last_element)
            last_element = x
            return False
    
    mask = df[col].map_elements(is_duplicated, return_dtype=pl.Boolean)
    return mask

def deduplicate(df: pl.DataFrame) -> pl.DataFrame:

    # 1. Remove total duplicates
    df = df.unique(maintain_order=True)

    # 2. Remove duplicates based on pmid runs
    mask = duplicated_run_mask(df, col='canonical')

    repeated_documents = df.filter(mask)
    df = df.filter(~mask)


    # On inspection, these 60 rows appear to be where the yaml is given twice: as a "final answer"
    suspicious = df.filter(
        df.select(['pmid', 'descriptor', 'substrate', 'kcat', 'km', 'kcat_km']).is_duplicated()
    ) # .sort('pmid', 'descriptor', 'substrate', 'kcat', 'km', 'kcat_km')


    # try to preserve the "final answer"
    df = df.unique(['pmid', 'descriptor', 'substrate', 'kcat', 'km', 'kcat_km'], keep='last', maintain_order=True)
    pass

    return df

def count_kcat_conventionally(df: pl.DataFrame) -> pl.DataFrame:
    # count kcat, in the conventional sense
    conventional_kcat_df = (
        df.select('pmid', 'enzyme', 'mutant', 'pH', 'temperature', 'kcat')
        .unique(['pmid', 'enzyme', 'mutant', 'pH', 'temperature', 'kcat'], maintain_order=True)
    ) # 195_277
    conventional_kcat_df = conventional_kcat_df.unique(maintain_order=True)

    return conventional_kcat_df

def deduplicate_with_context_df(
    data: pl.DataFrame,
    context_df: pl.DataFrame,
    unique_column = 'pmid'
) -> pl.DataFrame:
    """
    Deduplicate the data DataFrame, if context_df is available.

    Pre:
    - data: should have columns 'custom_id'
    - context_df: should have columns 'custom_id', {unique_column}

    Post:
    {unique_column} will be unique. If there are duplicates, the first one will be kept.
    """
    # bad_custom_ids = context_df.filter(
    #     context_df.select('custom_id').is_duplicated()
    # ).sort('custom_id')
    # print(bad_custom_ids) # 564
    # REASON: GPT provides two yamls: one, normally; the second one, the "final answer" (the exact same one).

    # doc_scanned_twice = context_df.unique([unique_column, 'custom_id'], keep='first')
    # doc_scanned_twice = doc_scanned_twice.filter(
    #     doc_scanned_twice.select(unique_column).is_duplicated()
    # ).sort('pmid')
    # print(doc_scanned_twice) # 5706

    safe_df = context_df.unique(unique_column, keep='first', maintain_order=True)

    return data.join(
        safe_df.select(['custom_id', unique_column]),
        on='custom_id',
        how='semi'
    )

def deduplicate_with_custom_id(
    data: pl.DataFrame,
    unique_column = 'canonical'

):
    """
    Deduplicate the data DataFrame, if custom_id is available.

    Pre:
    - data: should have columns {unique_column}, 'custom_id'
    """
    good_ids = data.select('custom_id', unique_column).unique(
        unique_column, keep='first', maintain_order=True
    )

    return data.join(
        good_ids.select(['custom_id', unique_column]),
        on='custom_id',
        how='semi'
    )

if __name__ == "__main__":
    df = pl.read_parquet('data/export/TheData_kcat.parquet')
    df = deduplicate(df)
    df.write_parquet('data/export/2_dedup/TheData_kcat_dedup.parquet')
