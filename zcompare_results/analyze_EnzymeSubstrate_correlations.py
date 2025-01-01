import polars as pl
from sklearn.metrics import mean_absolute_error, mean_squared_error

import numpy as np
from scipy import stats
from sklearn.metrics import r2_score
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_correlations(matched_view: pl.DataFrame, title):
    # matched_view = pl.read_parquet('_debug/cache/beluga_matched_based_on_EnzymeSubstrate.parquet')
    matched_view = matched_view.filter(
        ((pl.col('km_value_1') > 0) & (pl.col('km_value_2') > 0))
        | ((pl.col('kcat_value_1') > 0) & (pl.col('kcat_value_2') > 0))
    ).with_columns([
        (10 ** (pl.col('km_value_1').log10() - pl.col('km_value_2').log10()).abs()).alias('km_diff'),
        (10 ** (pl.col('kcat_value_1').log10() - pl.col('kcat_value_2').log10()).abs()).alias('kcat_diff'),
    ])
    # matched_view = matched_view.with_columns([
    #     ((pl.col('objective').cast(pl.Int16) % 10000) >= 6000).alias('same_enzyme'),
    # ])
    print("able to find same enzyme:", matched_view.filter(pl.col('same_enzyme')).height / matched_view.height)
    print("able to find same substrate:", matched_view.filter(pl.col('same_substrate')).height / matched_view.height)
    # compute the R^2 and spearman correlation based on the value of km1, km2, kcat1, kcat2
    corr_view = matched_view.filter(
        pl.col('same_substrate') 
        & pl.col('same_enzyme')
        & (pl.col('same_mutant').is_null() | pl.col('same_mutant'))
    ).select([
        'pmid', 'km_value_1', 'km_value_2', 'kcat_value_1', 'kcat_value_2', 'km_diff', 'kcat_diff'
    ])
    # corr_view = corr_view.drop_nulls()

    corr_view_km = corr_view.filter(
        (corr_view['km_value_1'] > 0) & (corr_view['km_value_2'] > 0)
    )
    km_1 = corr_view_km['km_value_1'].to_numpy()
    km_2 = corr_view_km['km_value_2'].to_numpy()

    corr_view_kcat = corr_view.filter(
        (corr_view['kcat_value_1'] > 0) & (corr_view['kcat_value_2'] > 0)
    )
    kcat_1 = corr_view_kcat['kcat_value_1'].to_numpy()
    kcat_2 = corr_view_kcat['kcat_value_2'].to_numpy()
    

    # log_ratio_threshold=2

    # def identify_outliers(arr1, arr2):
    #     # Calculate log10 ratios
    #     log_ratios = np.log10(arr2) - np.log10(arr1)
        
    #     # Identify outliers
    #     outlier_mask = np.abs(log_ratios) > log_ratio_threshold
        
    #     return ~outlier_mask, log_ratios
    
    # Identify outliers
    # km_valid_mask, km_log_ratios = identify_outliers(km_1, km_2)
    # kcat_valid_mask, kcat_log_ratios = identify_outliers(kcat_1, kcat_2)

    def calculate_metrics(arr1, arr2):
        """Calculate all metrics on log-transformed data"""
        log1, log2 = np.log10(arr1), np.log10(arr2)
        return {
            # 'count': len(arr1), # same as total_pairs
            'r2': r2_score(log1, log2),
            'pearson': stats.pearsonr(log1, log2)[0],
            'spearman': stats.spearmanr(arr1, arr2)[0],
            'mae': mean_absolute_error(log1, log2),  # log10 units
            'rmse': np.sqrt(mean_squared_error(log1, log2)),  # log10 units
            'accuracy': np.mean(np.abs(log1 - log2) < 0.0211892991) # count those within 5% error
        }

    results = {
        'km': {
            'total_pairs': len(km_1),
            'total_pmids': corr_view_km['pmid'].n_unique(),
            # 'valid_pairs': np.sum(km_vsalid_mask),
            # 'outliers': np.sum(~km_valid_mask),
            'original': calculate_metrics(km_1, km_2),
            # 'without_outliers': calculate_metrics(km_1[km_valid_mask], km_2[km_valid_mask])
        },
        'kcat': {
            'total_pairs': len(kcat_1),
            'total_pmids': corr_view_kcat['pmid'].n_unique(),
            # 'valid_pairs': np.sum(kcat_valid_mask),
            # 'outliers': np.sum(~kcat_valid_mask),
            'original': calculate_metrics(kcat_1, kcat_2),
            # 'without_outliers': calculate_metrics(kcat_1[kcat_valid_mask], kcat_2[kcat_valid_mask])
        }
    }

    # Print results
    
    for key, value in results.items():
        print(f'{key}:')
        for k, v in value.items():
            if isinstance(v, dict):
                print(f'  {k}:')
                for k2, v2 in v.items():
                    print(f'    {k2}: {v2}')
            else:
                print(f'  {k}: {v}')
    
    visualize_sns_scatter(corr_view, title)
    # visualize_sns_kde(corr_view, title)



def visualize_sns_scatter(df: pl.DataFrame, title, log_ratio_threshold=2):
    # Create masks and log transformations
    df = df.with_columns([
        pl.col('km_value_1').log10().alias('log_km_1'),
        pl.col('km_value_2').log10().alias('log_km_2'),
        pl.col('kcat_value_1').log10().alias('log_kcat_1'),
        pl.col('kcat_value_2').log10().alias('log_kcat_2'),
        (pl.col('km_diff') < 100).alias('km_valid'),
        (pl.col('kcat_diff') < 100).alias('kcat_valid'),
    ])

    df = df.filter(
        (pl.col('log_km_1').is_null() | pl.col('log_km_1').is_between(-10, 10))
        & (pl.col('log_km_2').is_null() | pl.col('log_km_2').is_between(-10, 10))
        & (pl.col('log_kcat_1').is_null() | pl.col('log_kcat_1').is_between(-10, 10))
        & (pl.col('log_kcat_2').is_null() | pl.col('log_kcat_2').is_between(-10, 10))
    )
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 15))
    ax1, ax2, ax3, ax4 = axes.flatten()

    # KM scatter plot
    sns.scatterplot(data=df, x="log_km_1", y="log_km_2", hue="km_valid", 
                    alpha=0.5, ax=ax1)

    # Save the limits set by Seaborn scatterplot
    x_min, x_max = ax1.get_xlim()
    # y_min, y_max = ax1.get_ylim()

    # Plot the lines
    # ax1.plot([x_min, x_max], [x_min, x_max], 'k--')  # Line y=x
    for dy in [-6, -3, 0, 3, 6]:
        ax1.plot([x_min, x_max], [x_min + dy, x_max + dy], color='gray', alpha=0.2)

    # Reset the axis limits to ensure the lines are visible
    # ax1.set_xlim(x_min, x_max)
    # ax1.set_ylim(y_min, y_max)
    ax1.set_title('Km Comparison')

    # KM histogram
    km_log_ratios = df["log_km_2"] - df["log_km_1"]
    sns.histplot(km_log_ratios, bins=50, kde=False, ax=ax2)
    ax2.axvline(x=log_ratio_threshold, color='r', linestyle='--')
    ax2.axvline(x=-log_ratio_threshold, color='r', linestyle='--')
    ax2.set_title('Km Log Ratio Distribution')
    
    # kcat scatter plot
    sns.scatterplot(data=df, x="log_kcat_1", y="log_kcat_2", hue="kcat_valid", 
                    alpha=0.5, ax=ax3)
    # ax3.plot([df["log_kcat_1"].min(), df["log_kcat_1"].max()], 
    #          [df["log_kcat_1"].min(), df["log_kcat_1"].max()], 'k--')
    # for dy in [-3]:
    #     ax3.plot([df["log_kcat_1"].min(), df["log_kcat_1"].max()],
    #              [df["log_kcat_1"].min() + dy, df["log_kcat_1"].max() + dy], color='gray', linestyle='--')
    # for dy in [np.log10(60)]:
    #     ax3.plot([df["log_kcat_1"].min(), df["log_kcat_1"].max()],
    #              [df["log_kcat_1"].min() + dy, df["log_kcat_1"].max() + dy], color='blue', linestyle='--')
    x_min, x_max = ax3.get_xlim()
    for dy in [-3, 0]:
        ax3.plot([x_min, x_max], [x_min + dy, x_max + dy], color='gray', alpha=0.2)
    for dy in [np.log10(60)]:
        ax3.plot([x_min, x_max], [x_min + dy, x_max + dy], color='blue', alpha=0.2)
    ax3.set_title('kcat Comparison')

    # kcat histogram
    kcat_log_ratios = df["log_kcat_2"] - df["log_kcat_1"]
    sns.histplot(kcat_log_ratios, bins=50, kde=False, ax=ax4)
    ax4.axvline(x=log_ratio_threshold, color='r', linestyle='--')
    ax4.axvline(x=-log_ratio_threshold, color='r', linestyle='--')
    ax4.set_title('kcat Log Ratio Distribution')

    # Overall title and layout adjustments
    fig.suptitle(title)
    plt.tight_layout()
    # plt.subplots_adjust(top=0.9)
    plt.subplots_adjust(left=0.08, right=0.95, bottom=0.08, top=0.91, wspace=0.26, hspace=0.26)
    plt.show()

def visualize_sns_kde(df: pl.DataFrame, title):
    # Add log transformations to the DataFrame
    df = df.with_columns([
        pl.col('km_value_1').log10().alias('log_km_1'),
        pl.col('km_value_2').log10().alias('log_km_2'),
        pl.col('kcat_value_1').log10().alias('log_kcat_1'),
        pl.col('kcat_value_2').log10().alias('log_kcat_2')
    ])

    # Create KDE plot for Km values
    g1 = sns.jointplot(data=df, x="log_km_1", y="log_km_2", kind="kde", fill=True, alpha=0.6)
    g1.ax_joint.plot(
        [df["log_km_1"].min(), df["log_km_1"].max()],
        [df["log_km_1"].min(), df["log_km_1"].max()],
        'k--', alpha=0.5
    )
    g1.fig.suptitle(f"{title} - Km Comparison")
    plt.subplots_adjust(top=0.9)

    # Create KDE plot for kcat values
    g2 = sns.jointplot(data=df, x="log_kcat_1", y="log_kcat_2", kind="kde", fill=True, alpha=0.6)
    g2.ax_joint.plot(
        [df["log_kcat_1"].min(), df["log_kcat_1"].max()],
        [df["log_kcat_1"].min(), df["log_kcat_1"].max()],
        'k--', alpha=0.5
    )
    g2.fig.suptitle(f"{title} - kcat Comparison")
    plt.subplots_adjust(top=0.9)
    
    plt.show()

def visualize_correlations(km_1, km_2, kcat_1, kcat_2, title, log_ratio_threshold=2):
    # Create visualization
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 15)) 

    ax1: plt.Axes
    ax2: plt.Axes
    ax3: plt.Axes
    ax4: plt.Axes
    
    # KM scatter plots
    ax1.scatter(np.log10(km_1[km_valid_mask]), np.log10(km_2[km_valid_mask]), 
                alpha=0.5, label='Valid points')
    ax1.scatter(np.log10(km_1[~km_valid_mask]), np.log10(km_2[~km_valid_mask]), 
                alpha=0.5, color='red', label='Outliers')
    # plot the line y=x
    ax1.plot([min(np.log_km_1), max(np.log_km_1)], 
             [min(np.log_km_1), max(np.log_km_1)], 'k--')
    # plot the line y=x+3
    for dy in [-6, -3, 3, 6]:
        ax1.plot([min(np.log_km_1), max(np.log_km_1)],
                [min(np.log_km_1) + dy, max(np.log_km_1) + dy], color='gray', linestyle='--')
    ax1.set_xlabel('log_km_1')
    ax1.set_ylabel('log10(Km_2)')
    ax1.set_title('Km Comparison')
    ax1.legend()
    
    # Km histogram of log ratios
    ax2.hist(km_log_ratios, bins=50)
    ax2.axvline(x=log_ratio_threshold, color='r', linestyle='--')
    ax2.axvline(x=-log_ratio_threshold, color='r', linestyle='--')
    ax2.set_xlabel('log10(Km_2/Km_1)')
    ax2.set_ylabel('Count')
    ax2.set_title('Km Log Ratio Distribution')
    
    # kcat scatter plots
    ax3.scatter(np.log10(kcat_1[kcat_valid_mask]), np.log10(kcat_2[kcat_valid_mask]), 
                alpha=0.5, label='Valid points')
    ax3.scatter(np.log10(kcat_1[~kcat_valid_mask]), np.log10(kcat_2[~kcat_valid_mask]), 
                alpha=0.5, color='red', label='Outliers')
    # plot the line y=x
    ax3.plot([min(np.log_kcat_1), max(np.log_kcat_1)], 
             [min(np.log_kcat_1), max(np.log_kcat_1)], 'k--')
    for dy in [-3]:
        ax3.plot([min(np.log_kcat_1), max(np.log_kcat_1)], # gray, not green
                [min(np.log_kcat_1) + dy, max(np.log_kcat_1) + dy], color='gray', linestyle='--')
    for dy in [np.log10(60)]:
        ax3.plot([min(np.log_kcat_1), max(np.log_kcat_1)],
                [min(np.log_kcat_1) + dy, max(np.log_kcat_1) + dy], color='blue', linestyle='--')
    
    ax3.set_xlabel('log_kcat_1')
    ax3.set_ylabel('log10(kcat_2)')
    ax3.set_title('kcat Comparison')
    ax3.legend()
    
    # kcat histogram of log ratios
    ax4.hist(kcat_log_ratios, bins=50)
    ax4.axvline(x=log_ratio_threshold, color='r', linestyle='--')
    ax4.axvline(x=-log_ratio_threshold, color='r', linestyle='--')
    ax4.set_xlabel('log10(kcat_2/kcat_1)')
    ax4.set_ylabel('Count')
    ax4.set_title('kcat Log Ratio Distribution')

    # overall title
    fig.suptitle(title)
    
    plt.tight_layout()
    

    

    # set subplot layout
    # bottom = 0.08, top = 0.95, hspace = 0.26
    plt.subplots_adjust(left=0.08, right=0.95, bottom=0.08, top=0.91, wspace=0.26, hspace=0.26)
    # Display the figures
    plt.show()
    pass

if __name__ == '__main__':





    # working = 'apogee'
    # working = 'beluga'
    # working = 'cherry-dev'
    # working = 'sabiork'
    # working = 'apatch'
    # working = 'bucket'
    working = 'everything'

    # against = 'runeem'
    against = 'brenda'
    # against = 'sabiork'

    # scino_only = True
    scino_only = False
    # scino_only = None
    # scino_only = 'false_revised'

    if scino_only is True:
        working += '_scientific_notation'
    elif scino_only is False:
        working += '_no_scientific_notation'
    elif scino_only == 'false_revised':
        working += '_no_scientific_revised'
    
    readme = f'data/matched/EnzymeSubstrate/{against}/{against}_{working}.parquet'
    matched_view = pl.read_parquet(readme)

    # matched_view = matched_view.filter(
    #     pl.col('pmid') != '21980421'
    # )
    print(readme)
    analyze_correlations(matched_view, f"1. {working} 2. {against}")
    