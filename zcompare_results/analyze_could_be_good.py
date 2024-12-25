import polars as pl

def main():

    working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    # working = 'everything'

    # against = 'runeem'
    against = 'brenda'

    # scino_only = True
    # scino_only = False
    scino_only = None

    if scino_only:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'

    readme = f'data/matched/EnzymeSubstrate/{against}/{against}_{working}.parquet'
    matched_view = pl.read_parquet(readme)

    stuff = matched_view.filter(
        (pl.col('kcat_diff').is_between(0.9, 1.05)) |
        pl.col('kcat_diff').is_between(59, 60)
        | pl.col('kcat_diff').is_between(3590, 3610)
        | (pl.col('kcat_1').str.contains('ms') & pl.col('kcat_diff').is_between(999000, 1001000))
        | (pl.col('kcat_1').str.contains('ms') & pl.col('kcat_diff').is_between(999, 1001))
    )
    # 27357 rows
    # 344 misconvert rows
    print(stuff)

if __name__ == "__main__":
    main()