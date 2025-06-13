from typing import Tuple
import polars as pl

from enzyextract.dependency.injection import REQUIRE, resolve
from enzyextract.dependency.prereqs import export
from enzyextract.post.finale.deduplication import count_kcat_conventionally, deduplicate


def highly_duplicated_separately(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
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

def highly_duplicated(df: pl.DataFrame) -> pl.DataFrame:
    """
    Find highly duplicated rows in the dataframe.

    Deduplicate on basis of the tuple (kcat, km).
    """

    # filter out rows where both kcat and km are null
    df = df.filter(
        pl.col('kcat').is_not_null() | pl.col('km').is_not_null()
    )

    stats = df.group_by(['pmid']).agg(
        pl.struct('kcat', 'km').count().alias('count'),
        pl.struct('kcat', 'km').n_unique().alias('unique_count')
    ).with_columns(
        (pl.col('unique_count') / pl.col('count')).alias('unique_percent')
    ).sort('unique_percent')

    return stats

@resolve
@export("data/export/2_dedup/TheData_dup_stats.parquet")
@export("data/export/2_dedup/TheData_kcat_dedup.parquet")
@export("data/export/2_dedup/TheData_kcat_duplicated.parquet")
def script_detect_extreme_duplication(
    df = REQUIRE('data/export/TheData_kcat.parquet')
):
    # df = pl.read_parquet('data/export/TheData_kcat.parquet')
    df = deduplicate(df)
    # kcat_stats, km_stats = highly_duplicated(df)

    # stats = pl.concat([
    #     kcat_stats.with_columns(
    #         pl.lit('kcat').alias('type')
    #     ),
    #     km_stats.with_columns(
    #         pl.lit('km').alias('type')
    #     )
    # ]).sort('unique_percent')
    stats = highly_duplicated(df)
    # filter out those with high duplication
    suspicious = pl.concat([
        stats.filter(
            pl.col('unique_percent') < 1/7
        ).select('pmid', 'unique_percent'),
    ]).unique()

    deduplicated_df = df.join(
        suspicious,
        on='pmid',
        how='anti'
    )

    duplication_df = df.join(
        stats,
        on='pmid',
        how='left'
        # how='semi'
    ).select(
        'pmid',
        pl.selectors.exclude(
            'pmid',
            'cid', 
            'brenda_id', 
            'smiles', 
            'cid_full', 
            'brenda_id_full', 
            'enzyme_ecs', 
            'sequence', 
            'sequence_source', 
            'uniprot', 
            'ncbi', 
            'pdb', 
            'max_enzyme_similarity', 
            'max_organism_similarity', 
            'total_similarity'
        )
    )
    stats.write_parquet('data/export/2_dedup/TheData_dup_stats.parquet')
    deduplicated_df.write_parquet('data/export/2_dedup/TheData_kcat_dedup.parquet')
    duplication_df.write_parquet('data/export/2_dedup/TheData_kcat_duplicated.parquet')

    conventional_kcat_df = count_kcat_conventionally(deduplicated_df)
    print(conventional_kcat_df.height)

    # Here are the changes after deduplication:
    # 1. Documents processed twice are removed
    # 2. PMIDs where <1/7 of either kcat or km values are unique are removed

def attach_repetitive_flag(
    data: pl.DataFrame, 
    threshold: float = 0.35,
) -> pl.DataFrame:
    """
    Pre: 
    - data: should contain 'pmid' and 'descriptor' columns.

    Post:
    - data: additional column named 'flag.repetitive' will be added. If the row is suspected to be
    highly duplicated or repeated LLM-generated data, this `flag.repetitive` will report the
    percentage of repetitive data. If the row is not suspected, this column will be null.


    See also: the Repeat Curse, repetition penalty.
    """
    # those that are derived from XML are safe - remove from the analysis
    stats = highly_duplicated(data)
    # filter out those with high duplication

    reported = stats.filter(pl.col('unique_percent') < threshold)

    reported = reported.select(
        'pmid',
        (1 - pl.col('unique_percent')).alias('flag.repetitive')
    ).unique('pmid', keep='first')
    data = data.join(
        reported,
        on='pmid',
        how='left',
        validate='m:1'
    )
    return data

if __name__ == "__main__":
    script_detect_extreme_duplication()

