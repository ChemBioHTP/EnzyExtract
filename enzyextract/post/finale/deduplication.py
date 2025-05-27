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

if __name__ == "__main__":
    df = pl.read_parquet('data/export/TheData_kcat.parquet')
    df = deduplicate(df)
    df.write_parquet('data/export/2_dedup/TheData_kcat_dedup.parquet')