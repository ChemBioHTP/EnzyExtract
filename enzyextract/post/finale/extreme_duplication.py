import polars as pl

from enzyextract.post.finale.deduplication import count_kcat_conventionally, deduplicate


def highly_duplicated(df: pl.DataFrame) -> pl.DataFrame:
    """
    Find highly duplicated rows in the dataframe.

    Count kcat and unique kcat
    """
    kcat = df.filter(
        pl.col('kcat').is_not_null()
        & pl.col('kcat_value').is_not_null()
        & (pl.col('kcat_value') != 0)
    ).select('pmid', 'kcat')
    km = df.filter(
        pl.col('km').is_not_null()
        & pl.col('km_value').is_not_null()
        & (pl.col('km_value') != 0)
    ).select('pmid', 'km')

    kcat_stats = kcat.group_by('pmid').agg(
        pl.col('kcat').count().alias('count'),
        pl.col('kcat').n_unique().alias('unique_count')
    ).with_columns(
        (pl.col('unique_count') / pl.col('count')).alias('unique_percent')
    ).sort('unique_percent')

    km_stats = km.group_by('pmid').agg(
        pl.col('km').count().alias('count'),
        pl.col('km').n_unique().alias('unique_count')
    ).with_columns(
        (pl.col('unique_count') / pl.col('count')).alias('unique_percent')
    ).sort('unique_percent')

    return kcat_stats, km_stats


if __name__ == "__main__":
    df = pl.read_parquet('data/export/TheData_kcat.parquet')
    df = deduplicate(df)
    kcat_stats, km_stats = highly_duplicated(df)

    stats = pl.concat([
        kcat_stats.with_columns(
            pl.lit('kcat').alias('type')
        ),
        km_stats.with_columns(
            pl.lit('km').alias('type')
        )
    ]).sort('unique_percent')
    # filter out those with high duplication
    suspicious = pl.concat([
        stats.filter(
            pl.col('unique_percent') < 1/7
        ).select('pmid'),
    ]).unique()
    suspicious_df = df.join(
        suspicious,
        on='pmid',
        how='semi'
    )
    dedup_df = df.join(
        suspicious,
        on='pmid',
        how='anti'
    )
    stats.write_parquet('data/export/2_dedup/TheData_dup_stats.parquet')
    dedup_df.write_parquet('data/export/2_dedup/TheData_kcat_dedup.parquet')
    suspicious_df.write_parquet('data/export/2_dedup/TheData_kcat_xduplicated.parquet')

    conventional_kcat_df = count_kcat_conventionally(dedup_df)
    print(conventional_kcat_df.height)

    # Here are the changes after deduplication:
    # 1. Documents processed twice are removed
    # 2. PMIDs where <1/7 of either kcat or km values are unique are removed

