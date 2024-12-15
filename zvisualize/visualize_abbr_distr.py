"""
Visualize the distribution of string length with seaborn, to see if we can pick a threshold
to detect when a string is too long.
"""

import seaborn as sns
import matplotlib.pyplot as plt
import polars as pl

def script_see_abbr_dist():
    df = pl.read_parquet('data/synonyms/abbr/beluga_abbrs.parquet')

    # GPT has max full_name length of 155
    df = df.with_columns([
        (pl.col('b_name').str.len_chars() >= 156).alias('too_long'),
        pl.col('a_name').str.len_chars().alias('a_len'),
        pl.col('b_name').str.len_chars().alias('b_len'),
    ])

    # sns.histplot(df['a_len'], bins=50)
    # vert line at 156
    df_view = df.filter(pl.col('b_name').str.len_chars() >= 100)
    sns.histplot(df_view['b_len'], bins=50)
    plt.axvline(156, color='red')
    plt.show()

if __name__ == '__main__':
    script_see_abbr_dist()