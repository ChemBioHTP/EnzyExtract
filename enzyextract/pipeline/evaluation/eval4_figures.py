import matplotlib.pyplot as plt
import numpy as np
import polars as pl

from enzyextract.pipeline.evaluation.utilities import printred

def _figure_S1(data: pl.DataFrame):

    df = data.select(
        pl.col('temperature').str.extract('(-?\d+(\.\d*)?) *°C', 1).cast(pl.Float64),
            # pl.col('temperature').str.extract_all('(\d+(\.\d*)?)')
            # .list.eval(
            #     pl.element().cast(pl.Float64)
            # ).list.mean(), # if there is a range (ie. 20 to 30), take the mean
        pl.col('pH')

            # .str.extract('\d+(\.\d*)?', 0) # extract the first number, as pH is usually a single value
            # .cast(pl.Float64)
            .str.extract_all('\d+(\.\d*)?')
            .list.eval(
                pl.element().cast(pl.Float64)
            ).list.mean(), # if there is a range (ie. 6.5 to 7.5), take the mean
        
        pl.coalesce(
            # prefer fullname if available (more specific)
            pl.col('enzyme_ecs_full')
            .list.eval(
                pl.element().str.extract('^(\d+)\.')
            ).list.unique(),
            pl.col('enzyme_ecs')
            .list.eval(
                pl.element().str.extract('^(\d+)\.')
            ).list.unique()
        ).alias('ec_first_digit')
        
    ) # .to_pandas()

    # remove ambiguous ECs (when there are multiple first digits)
    df = df.with_columns(
        pl.when(pl.col('ec_first_digit').list.len() > 1)
        .then(None)
        .otherwise(pl.col('ec_first_digit').list.first())
        .alias('ec_first_digit')
    )

    print(df.describe())


    # Create subplots
    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.5))

    # Temperature Distribution
    axes[0].hist(df['temperature'], bins=30, range=(-80, 200), color='skyblue', edgecolor='black', alpha=0.7)
    axes[0].set_title('Temperature Distribution')
    axes[0].set_xlabel('Temperature (°C)')
    axes[0].set_ylabel('Frequency')
    # axes[0].set_xlim(-100, 200)
    axes[0].grid(True, alpha=0.3)

    # pH Distribution
    axes[1].hist(df['pH'], bins=20, range=(0, 14), color='lightgreen', edgecolor='black', alpha=0.7)
    axes[1].set_title('pH Distribution')
    axes[1].set_xlabel('pH Value')
    axes[1].set_ylabel('Frequency')
    # axes[1].set_xlim(0, 14)
    axes[1].grid(True, alpha=0.3)

    # EC Number Distribution
    ec_counts = df['ec_first_digit'].value_counts().sort('ec_first_digit', descending=False).drop_nulls('ec_first_digit')
    axes[2].bar(ec_counts['ec_first_digit'], ec_counts['count'], color='gold', edgecolor='black', alpha=0.7)
    axes[2].set_title('EC Number Distribution')
    axes[2].set_xlabel('EC First Digit')
    axes[2].set_ylabel('Frequency')
    axes[2].grid(True, alpha=0.3)

    # count EC numbers
    printred("EC count", df.select('ec_first_digit').drop_nulls().height)

    # Count each EC first digit
    print(ec_counts)

    plt.tight_layout()
    plt.savefig('data/export/figures/figure_S1.svg', bbox_inches='tight') # dpi=300, 
    plt.show()

def _figure_S2(data: pl.DataFrame):
    """
    figure S2: kcat and Km distributions (log10)
    """
    # Filter for positive values only
    kcat = data.filter(pl.col('kcat_value') > 0)['kcat_value'].to_numpy()
    km = data.filter(pl.col('km_value') > 0)['km_value'].to_numpy()

    # Take log10
    log_kcat = np.log10(kcat)
    log_km = np.log10(km)

    # Create subplots
    fig, axes = plt.subplots(1, 2, figsize=(7, 3.5))

    # kcat Distribution
    axes[0].hist(log_kcat, bins=30, range=(-8, 10), color='skyblue', edgecolor='black', alpha=0.7)
    axes[0].set_title('$k_{cat}$ Distribution (log10)')
    axes[0].set_xlabel(r'log$_{10} (k_{cat})$ [s$^{-1}$]')
    axes[0].set_ylabel('Frequency')
    axes[0].grid(True, alpha=0.3)

    # Km Distribution
    axes[1].hist(log_km, bins=30, range=(-14, 7), color='lightgreen', edgecolor='black', alpha=0.7)
    axes[1].set_title('$K_m$ Distribution (log10)')
    axes[1].set_xlabel(r'log$_{10} (K_m)$ [mM]')
    axes[1].set_ylabel('Frequency')
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('data/export/figures/figure_S2.svg', bbox_inches='tight') # dpi=300, 
    plt.show()



if __name__ == '__main__':
    # data = pl.read_parquet('data/export/TheData_pruned.parquet')
    data = pl.read_parquet('data/export/TheData_pruned.parquet').filter(
        pl.col('kcat').is_not_null()
    )

    
    _figure_S1(data)
    # _figure_S2(data)
