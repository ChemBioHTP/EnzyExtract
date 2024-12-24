import polars as pl
from sklearn.metrics import mean_absolute_error, mean_squared_error
def analyze_correlations(matched_view: pl.DataFrame, title: str):
    # matched_view = pl.read_parquet('_debug/cache/beluga_matched_based_on_EnzymeSubstrate.parquet')
    matched_view = matched_view.filter(
        (pl.col('km_value_1') > 0)
        & (pl.col('km_value_2') > 0)
        & (pl.col('kcat_value_1') > 0)
        & (pl.col('kcat_value_2') > 0)
    ).with_columns([
        (10 ** (pl.col('km_value_1').log10() - pl.col('km_value_2').log10()).abs()).alias('km_diff'),
        (10 ** (pl.col('kcat_value_1').log10() - pl.col('kcat_value_2').log10()).abs()).alias('kcat_diff'),
    ])

    print("able to find same enzyme:", matched_view.filter(pl.col('same_enzyme')).height / matched_view.height)
    print("able to find same substrate:", matched_view.filter(pl.col('same_substrate')).height / matched_view.height)
    # compute the R^2 and spearman correlation based on the value of km1, km2, kcat1, kcat2
    corr_view = matched_view.filter(
        pl.col('same_substrate') 
        & pl.col('same_enzyme')
        & (pl.col('same_mutant').is_null() | pl.col('same_mutant'))
    ).select([
        'pmid', 'km_value_1', 'km_value_2', 'kcat_value_1', 'kcat_value_2'
    ])
    corr_view = corr_view.drop_nulls()


    km_1 = corr_view['km_value_1'].to_numpy()
    km_2 = corr_view['km_value_2'].to_numpy()
    kcat_1 = corr_view['kcat_value_1'].to_numpy()
    kcat_2 = corr_view['kcat_value_2'].to_numpy()
    
    import numpy as np
    from scipy import stats
    from sklearn.metrics import r2_score
    import matplotlib.pyplot as plt
    log_ratio_threshold=2

    def identify_outliers(arr1, arr2):
        # Calculate log10 ratios
        log_ratios = np.log10(arr2) - np.log10(arr1)
        
        # Identify outliers
        outlier_mask = np.abs(log_ratios) > log_ratio_threshold
        
        return ~outlier_mask, log_ratios
    
    # Identify outliers
    km_valid_mask, km_log_ratios = identify_outliers(km_1, km_2)
    kcat_valid_mask, kcat_log_ratios = identify_outliers(kcat_1, kcat_2)

    def calculate_metrics(arr1, arr2):
        """Calculate all metrics on log-transformed data"""
        log1, log2 = np.log10(arr1), np.log10(arr2)
        return {
            'r2': r2_score(log1, log2),
            'pearson': stats.pearsonr(log1, log2)[0],
            'spearman': stats.spearmanr(arr1, arr2)[0],
            'mae': mean_absolute_error(log1, log2),  # log10 units
            'rmse': np.sqrt(mean_squared_error(log1, log2)),  # log10 units
            'accuracy': np.mean(np.abs(log1 - log2) < 0.0211892991) # those within 5% error
        }

    results = {
        'km': {
            'total_pairs': len(km_1),
            'valid_pairs': np.sum(km_valid_mask),
            'outliers': np.sum(~km_valid_mask),
            'original': calculate_metrics(km_1, km_2),
            # 'without_outliers': calculate_metrics(km_1[km_valid_mask], km_2[km_valid_mask])
        },
        'kcat': {
            'total_pairs': len(kcat_1),
            'valid_pairs': np.sum(kcat_valid_mask),
            'outliers': np.sum(~kcat_valid_mask),
            'original': calculate_metrics(kcat_1, kcat_2),
            # 'without_outliers': calculate_metrics(kcat_1[kcat_valid_mask], kcat_2[kcat_valid_mask])
        }
    }
    
    # Create visualization
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 15))
    
    # KM scatter plots
    ax1.scatter(np.log10(km_1[km_valid_mask]), np.log10(km_2[km_valid_mask]), 
                alpha=0.5, label='Valid points')
    ax1.scatter(np.log10(km_1[~km_valid_mask]), np.log10(km_2[~km_valid_mask]), 
                alpha=0.5, color='red', label='Outliers')
    ax1.plot([min(np.log10(km_1)), max(np.log10(km_1))], 
             [min(np.log10(km_1)), max(np.log10(km_1))], 'k--')
    ax1.set_xlabel('log10(Km_1)')
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
    ax3.plot([min(np.log10(kcat_1)), max(np.log10(kcat_1))], 
             [min(np.log10(kcat_1)), max(np.log10(kcat_1))], 'k--')
    ax3.set_xlabel('log10(kcat_1)')
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
    

    # set subplot layout
    # bottom = 0.08, top = 0.95, hspace = 0.26
    plt.subplots_adjust(left=0.08, right=0.95, bottom=0.08, top=0.91, wspace=0.26, hspace=0.26)
    # Display the figures
    plt.show()
    pass

if __name__ == '__main__':

    readme = 'data/matched/EnzymeSubstrate/brenda/brenda_apogee.parquet'
    # readme = 'data/matched/EnzymeSubstrate/brenda/brenda_apogee_no_scientific_notation.parquet'
    # readme = 'data/matched/EnzymeSubstrate/runeem/runeem_apogee_no_scientific_notation.parquet'
    # readme = 'data/matched/EnzymeSubstrate/runeem/runeem_apogee.parquet'
    # readme = 'data/matched/EnzymeSubstrate/runeem/runeem_beluga.parquet'
    # readme = 'data/matched/EnzymeSubstrate/runeem/runeem_beluga_no_scientific_notation.parquet'
    matched_view = pl.read_parquet(readme)
    print(readme)
    analyze_correlations(matched_view, readme)
    