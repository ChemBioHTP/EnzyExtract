import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

def visualize_ec():
    df = pl.read_parquet('data/export/TheData_kcat.parquet')

    # visualize distributions of the kcat, km, and EC values

    df = df.select('enzyme_ecs', 'enzyme_ecs_full', 'kcat_value', 'km_value').with_columns(
        pl.coalesce(
            pl.col('enzyme_ecs_full'),
            pl.col('enzyme_ecs'),
        ).alias('ecs')
    ).with_columns(
        pl.col('ecs').list.eval(
            pl.element().str.split('.').list.get(0)
        ).alias('ec1')
    ).filter(
        pl.col('ec1').list.len() == 1 # only keep unambiguous EC numbers
    ).with_columns(
        pl.col('ec1').list.get(0, null_on_oob=True).alias('ec1')
    ).filter(pl.col('ec1').is_not_null()).select('ec1', 'kcat_value', 'km_value').sort('ec1')


    ours_ec_pct = df.group_by('ec1').len().with_columns(
        (pl.col('len') / pl.col('len').sum() * 100).alias('percent'),
        pl.lit('ours').alias('source')
    )

    print(ours_ec_pct)

    # now do the same for brenda
    brenda = pl.read_parquet('data/brenda/brenda_kcat_v3slim.parquet').filter(
        pl.col('turnover_number').is_not_null()
    )

    brenda = brenda.with_columns([
        pl.col('ec').str.split('.').list.get(0).alias('ec1'),
        pl.col('ec').str.split('.').list.get(1).alias('ec2'),
        pl.col('ec').str.split('.').list.get(2).alias('ec3'),
        pl.col('ec').str.split('.').list.get(3).alias('ec4'),
    ]).sort('ec1')
    df_brenda = brenda.filter(pl.col('ec1').is_not_null()).select('ec1').with_columns(
        pl.lit('brenda').alias('source')
    )

    # Calculate the percentage of each ec1 category in brenda
    brenda_ec_pct = df_brenda.group_by('ec1').len().with_columns(
        (pl.col('len') / pl.col('len').sum() * 100).alias('percent'),
        pl.lit('brenda').alias('source')
    )

    # Combine the two dataframes
    # combined = ours_ec_pct.join(brenda_ec_pct, on='ec1', how='inner')
    combined = pl.concat([ours_ec_pct, brenda_ec_pct])
    print(combined)
    combined = combined.sort('ec1')

    # Plot the data
    sns.barplot(data=combined, x='ec1', y='percent', hue='source', dodge=True)

    # title: Distribution of EC1 numbers in our data versus BRENDA, kcat only
    plt.title('Distribution of EC1 numbers in our data versus BRENDA, kcat only\n (skip conflicting EC)')
    plt.savefig('ec_distribution.svg', format='svg')
    plt.show()

def visualize_kcat():
    ours = pl.read_parquet('data/export/TheData_kcat.parquet').select('kcat_value').with_columns(
        pl.col('kcat_value').log10().alias('log10_kcat')
    )

    # plot distribution of kcat
    sns.histplot(ours, x='log10_kcat', bins=100)

    plt.title('Distribution of log10 kcat values from EnzyExtract')
    plt.savefig('kcat_distribution.svg', format='svg')
    plt.show()

def visualize_km():
    ours = pl.read_parquet('data/export/TheData.parquet').select('km_value').with_columns(
        pl.col('km_value').log10().alias('log10_km')
    )

    # plot distribution of kcat, range: -20 to 20
    sns.histplot(ours, x='log10_km', bins=100, binrange=(-20, 20))

    plt.title('Distribution of log10 km values from EnzyExtract')
    plt.savefig('km_distribution.svg', format='svg')
    plt.show()






if __name__ == '__main__':
    visualize_ec()
    visualize_kcat()
    visualize_km()