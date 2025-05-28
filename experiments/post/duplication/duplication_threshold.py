# given data/export/2_dedup/TheData_kcat_hallucinations.parquet
# and given data/export/2_dedup/TheData_dup_stats.parquet

# plot a histogram of distribution of value "suspiciousness" (for hallucinations)
# also plot a histogram of "unique_percent" for TheData_dup_stats.parquet

# I would like to completely separate the plotting for hallucinations and duplications. (Two plt.show() calls.) For hallucinations, I want both a suspiciousness histogram and also a curve of number of suspicious data points (y) that remain given a certain threshold (x).

# For duplications, scratch all that before. I want total count on x axis, and unique percent on y axis. For clarity, only plot <0.9 percent unique. I want this to be a scatter plot. I also plan to eliminate extremely duplicated data, so plot the number of duplicated data points eliminated given a certain percent unique threshold (x).


import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

def script_analyze_hallucination():
    data_dir = Path("data/export/2_dedup")
    hallucinations_file = data_dir / "TheData_kcat_hallucinations.parquet"

    # Load the data
    df_hallucinations = pl.read_parquet(hallucinations_file)

    # HALLUCINATIONS ANALYSIS
    print("=== HALLUCINATIONS ANALYSIS ===")
    suspiciousness_data = df_hallucinations.select("suspiciousness").to_pandas()

    # Create figure for hallucinations
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # Plot 1: Histogram of suspiciousness
    ax1.hist(suspiciousness_data["suspiciousness"], bins=50, alpha=0.7, edgecolor='black')
    ax1.set_title("Distribution of Hallucinations")
    ax1.set_xlabel(r"False Positive Rate (Percent of Numeric Values Not In Text)")
    ax1.set_ylabel("Frequency")
    ax1.grid(True, alpha=0.3)

    # Plot 2: Threshold curve - number of suspicious data points remaining
    thresholds = np.linspace(suspiciousness_data["suspiciousness"].min(), 
                            suspiciousness_data["suspiciousness"].max(), 100)
    remaining_points = [len(suspiciousness_data[suspiciousness_data["suspiciousness"] >= thresh]) 
                    for thresh in thresholds]

    ax2.plot(thresholds, remaining_points, linewidth=2, color='red')
    ax2.set_title("Suspicious Data Points vs Threshold")
    ax2.set_xlabel("Threshold")
    ax2.set_ylabel("Rows Excluded at Threshold")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    # Print hallucinations statistics
    print("Suspiciousness Statistics:")
    print(df_hallucinations.select("suspiciousness").describe())

# Set up paths
def script_analyze_duplication():
    data_dir = Path("data/export/2_dedup")
    dup_stats_file = data_dir / "TheData_dup_stats.parquet"

    # Load the data
    df_dup_stats = pl.read_parquet(dup_stats_file)

    # DUPLICATION ANALYSIS
    print("\n=== DUPLICATION ANALYSIS ===")
    dup_stats_pandas = df_dup_stats.select(["unique_percent", "count", "unique_count"]).to_pandas()

    # Filter for unique_percent < 90%
    filtered_dup_stats = dup_stats_pandas[dup_stats_pandas["unique_percent"] < 90]

    # Create figure for duplications
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

    # Plot 1: Scatter plot - total count vs unique count (filtered)
    ax1.scatter(filtered_dup_stats["count"], filtered_dup_stats["unique_count"], alpha=0.5)
    ax1.set_title("kcat and Km: Total Count vs Unique Count")
    ax1.set_xlabel("Total Count")
    ax1.set_ylabel("Unique Count")
    ax1.grid(True, alpha=0.3)

    # Plot 3: Density plot - total count vs unique count (filtered)
    # ax3 = fig.add_subplot(1, 3, 3)  # Add a third subplot
    sns.kdeplot(x=filtered_dup_stats["count"], y=filtered_dup_stats["unique_count"],
                fill=True, cmap="viridis", ax=ax3)
    ax3.set_title("kcat and Km: Density Plot of Total Count vs Unique Count")
    ax3.set_xlabel("Total Count")
    ax3.set_ylabel("Unique Count")
    ax3.set_xlim(0, 50)
    ax3.set_ylim(0, 50)
    ax3.grid(True, alpha=0.3)

    # Plot 2: Elimination curve - duplicated data points eliminated by threshold
    unique_thresholds = np.linspace(0, 0.9, 100)
    eliminated_points = []

    for thresh in unique_thresholds:
        # Data points that would be eliminated (unique_percent < threshold)
        eliminated_data = dup_stats_pandas[dup_stats_pandas["unique_percent"] < thresh]
        # Calculate total duplicated points eliminated
        total_eliminated = eliminated_data["count"].sum() if len(eliminated_data) > 0 else 0
        eliminated_points.append(total_eliminated)

    ax2.plot(unique_thresholds, eliminated_points, linewidth=2, color='orange')
    ax2.set_title("Data Points Eliminated vs Unique Percent Threshold")
    ax2.set_xlabel("Unique Percent Threshold")
    ax2.set_ylabel("Number of Rows Below Threshold")
    ax2.grid(True, alpha=0.3)


    # Plot 4: Elimination curve clipped to [0, 0.2]
    # Find indices where unique_thresholds <= 0.2
    clip_mask = unique_thresholds <= 0.2
    unique_thresholds_clipped = unique_thresholds[clip_mask]
    eliminated_points_clipped = np.array(eliminated_points)[clip_mask]

    ax4.plot(unique_thresholds_clipped, eliminated_points_clipped, linewidth=2, color='red')
    ax4.set_title("Data Points Eliminated vs Unique Percent Threshold (0-20%)")
    ax4.set_xlabel("Unique Percent Threshold")
    ax4.set_ylabel("Number of Rows Below Threshold")
    ax4.grid(True, alpha=0.3)
    

    plt.tight_layout()
    plt.show()

    # Print duplication statistics
    print("Duplication Stats (All data):")
    print(df_dup_stats.select(["unique_percent", "count"]).describe())
    print(f"\nFiltered data (<90% unique): {len(filtered_dup_stats)} out of {len(dup_stats_pandas)} entries")

import polars as pl
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from mpl_toolkits.mplot3d import Axes3D
from scipy.stats import pearsonr, spearmanr

def script_analyze_combined_thresholds(unique_threshold=0.9, suspicious_threshold=0.1,
                                       unique_range=(0.0, 0.5), suspicious_range=(0.0, 1.0)):
    """
    Create a 3D plot showing how the number of filtered data points varies
    across different combinations of unique and suspicious thresholds (grid search)
    """
    data_dir = Path("data/export/2_dedup")
    hallucinations_file = data_dir / "TheData_kcat_hallucinations.parquet"
    duplicates_file = data_dir / "TheData_kcat_duplicated.parquet"
    
    # Load the datasets
    df_hallucinations = pl.read_parquet(hallucinations_file)
    df_duplicates = pl.read_parquet(duplicates_file)
    
    print("=== DATASET SCHEMAS ===")
    # print("Hallucinations columns:", df_hallucinations.columns)
    # print("Duplication stats columns:", df_duplicates.columns)
    
    # Find common columns (excluding the analysis columns)
    halluc_cols = set(df_hallucinations.columns)
    dup_cols = set(df_duplicates.columns)
    
    # Remove analysis-specific columns
    analysis_cols = {'suspiciousness', 'unique_percent', 'count', 'unique_count'}
    halluc_data_cols = halluc_cols - analysis_cols
    dup_data_cols = dup_cols - analysis_cols
    
    common_cols = list(halluc_data_cols.intersection(dup_data_cols))
    
    print(f"Common data columns for joining: {common_cols}")
    
    if not common_cols:
        print("Warning: No common columns found. Using row indices instead.")
        # If no common columns, we'll assume both datasets have the same row order
        # and create a simple index-based join
        df_hallucinations = df_hallucinations.with_row_count("row_id")
        df_duplicates = df_duplicates.with_row_count("row_id")
        join_key = "row_id"
    else:
        # Use common columns as join key
        join_key = common_cols
    
    # Join the datasets
    if isinstance(join_key, list):
        df_combined = df_hallucinations.join(
            df_duplicates, 
            on=join_key, 
            how="full"
        )
    else:
        df_combined = df_hallucinations.join(
            df_duplicates, 
            on=join_key, 
            how="full"
        )
    
    print(f"Combined dataset shape: {df_combined.shape}")


    # === BEGIN ANALYSIS ===

    # ==== CORRELATION ====

    # Plot correlation between unique_percent and suspiciousness (Pearson and Spearman)

    # Filter out null values for correlation analysis
    df_common = df_combined.filter(
        pl.col("unique_percent").is_not_null() &
        # (pl.col("unique_percent") <= 0.4) &
        pl.col("suspiciousness").is_not_null()
    )
    # Create scatter plot of unique_percent vs suspiciousness
    fig3, ax3 = plt.subplots(figsize=(10, 8))
    
    # Extract data for plotting (matplotlib supports polars directly)
    unique_percent = df_common.select("unique_percent").to_series()
    suspiciousness = df_common.select("suspiciousness").to_series()
    
    ax3.scatter(unique_percent, suspiciousness, alpha=0.6, s=20)
    ax3.set_xlabel('Unique Percent')
    ax3.set_ylabel('Suspiciousness')
    ax3.set_title('Correlation: Unique Percent vs Suspiciousness')
    ax3.grid(True, alpha=0.3)
    
    pearson_corr = df_common.select(pl.corr("unique_percent", "suspiciousness")).item()
    spearman_corr = df_common.select(pl.corr("unique_percent", "suspiciousness", method="spearman")).item()
    ax3.text(0.05, 0.95, f'Pearson: {pearson_corr:.4f}\nSpearman: {spearman_corr:.4f}',
             transform=ax3.transAxes, fontsize=12, verticalalignment='top',
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='black'))

    plt.tight_layout()
    plt.show()

    # ==== GRID SEARCH ====

    # Define threshold ranges for grid search
    unique_thresholds = np.linspace(unique_range[0], unique_range[1], 20)  # 20 points from 0.1 to 1.0
    suspicious_thresholds = np.linspace(suspicious_range[0], suspicious_range[1], 20)  # 20 points from 0.0 to 0.5

    # Create meshgrid for thresholds
    U_thresh, S_thresh = np.meshgrid(unique_thresholds, suspicious_thresholds)
    
    # Initialize array to store filtered counts
    filtered_counts = np.zeros_like(U_thresh)
    
    print("Performing grid search over threshold combinations...")
    
    # Calculate filtered counts for each threshold combination
    for i, u_thresh in enumerate(unique_thresholds):
        for j, s_thresh in enumerate(suspicious_thresholds):
            # Apply both thresholds (OR condition as per your revision)
            df_filtered = df_combined.filter(
                (pl.col("unique_percent") <= u_thresh) |
                (pl.col("suspiciousness") >= s_thresh)
            )
            filtered_counts[j, i] = df_filtered.height
    
    # Create 3D surface plot
    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot(111, projection='3d')
    
    # Create surface plot
    surf = ax.plot_surface(
        U_thresh, S_thresh, filtered_counts,
        cmap='viridis', 
        alpha=0.8,
        linewidth=0,
        antialiased=True
    )
    
    # Add contour lines at the base
    contours = ax.contour(U_thresh, S_thresh, filtered_counts, 
                         zdir='z', offset=0, cmap='viridis', alpha=0.5)
    
    # Set labels and title
    ax.set_xlabel('Unique Percent Threshold')
    ax.set_ylabel('Suspiciousness Threshold')
    ax.set_zlabel('Number of Filtered Data Points')
    ax.set_title('Grid Search: Filtered Data Points vs Threshold Combinations\n(unique_percent ≤ threshold OR suspiciousness ≥ threshold)')
    
    # Add colorbar
    plt.colorbar(surf, ax=ax, shrink=0.5, aspect=5, label='Filtered Count')
    
    # Add specific threshold combination marker if provided
    if unique_threshold <= 1.0 and suspicious_threshold <= 0.5:
        specific_filtered = df_combined.filter(
            (pl.col("unique_percent") <= unique_threshold) |
            (pl.col("suspiciousness") >= suspicious_threshold)
        ).height
        
        ax.scatter([unique_threshold], [suspicious_threshold], [specific_filtered],
                  color='red', s=100, alpha=1.0, label=f'Selected: ({unique_threshold}, {suspicious_threshold})')
        ax.legend()
    
    plt.tight_layout()
    plt.show()
    
    # Create a 2D heatmap as well for better visualization
    fig2, ax2 = plt.subplots(figsize=(12, 8))
    
    heatmap = ax2.imshow(filtered_counts, 
                        extent=[unique_thresholds.min(), unique_thresholds.max(),
                               suspicious_thresholds.min(), suspicious_thresholds.max()],
                        origin='lower', aspect='auto', cmap='viridis')
    
    # Add contour lines
    contour_lines = ax2.contour(U_thresh, S_thresh, filtered_counts, 
                               colors='white', alpha=0.6, linewidths=1)
    ax2.clabel(contour_lines, inline=True, fontsize=8, fmt='%d')
    
    ax2.set_xlabel('Unique Percent Threshold')
    ax2.set_ylabel('Suspiciousness Threshold')
    ax2.set_title('Heatmap: Filtered Data Points vs Threshold Combinations')
    
    plt.colorbar(heatmap, ax=ax2, label='Filtered Count')
    
    # Mark the specific threshold combination
    if unique_threshold <= 1.0 and suspicious_threshold <= 0.5:
        ax2.plot(unique_threshold, suspicious_threshold, 'ro', markersize=10, 
                label=f'Selected: ({unique_threshold}, {suspicious_threshold})')
        ax2.legend()
    
    plt.tight_layout()
    plt.show()
    

    

    

    # Print summary statistics
    # print("\n=== GRID SEARCH RESULTS ===")
    # print(f"Threshold ranges tested:")
    # print(f"  Unique percent: {unique_thresholds.min():.2f} to {unique_thresholds.max():.2f}")
    # print(f"  Suspiciousness: {suspicious_thresholds.min():.2f} to {suspicious_thresholds.max():.2f}")
    # print(f"Maximum filtered points: {filtered_counts.max()}")
    # print(f"Minimum filtered points: {filtered_counts.min()}")
    
    # # Find optimal threshold combinations
    # max_idx = np.unravel_index(np.argmax(filtered_counts), filtered_counts.shape)
    # min_idx = np.unravel_index(np.argmin(filtered_counts), filtered_counts.shape)
    
    # print(f"Maximum at: unique={unique_thresholds[max_idx[1]]:.3f}, suspicious={suspicious_thresholds[max_idx[0]]:.3f}")
    # print(f"Minimum at: unique={unique_thresholds[min_idx[1]]:.3f}, suspicious={suspicious_thresholds[min_idx[0]]:.3f}")
    
    return df_combined, U_thresh, S_thresh, filtered_counts

def explore_threshold_combinations():
    """
    Explore different threshold combinations to find optimal values
    """
    data_dir = Path("data/export/2_dedup")
    hallucinations_file = data_dir / "TheData_kcat_hallucinations.parquet"
    dup_stats_file = data_dir / "TheData_dup_stats.parquet"
    
    df_hallucinations = pl.read_parquet(hallucinations_file)
    df_dup_stats = pl.read_parquet(dup_stats_file)
    
    # Quick join (assuming same row order if no common columns)
    min_rows = min(df_hallucinations.height, df_dup_stats.height)
    df_combined = pl.concat([
        df_hallucinations.head(min_rows),
        df_dup_stats.head(min_rows).select(["unique_percent", "count", "unique_count"])
    ], how="horizontal")
    
    print("=== THRESHOLD EXPLORATION ===")
    
    # Test different threshold combinations
    unique_thresholds = [0.1, 0.2, 0.5, 0.8, 0.9]
    suspicious_thresholds = [0.05, 0.1, 0.2, 0.3, 0.5]
    
    results = []
    for ut in unique_thresholds:
        for st in suspicious_thresholds:
            filtered_count = df_combined.filter(
                (pl.col("unique_percent") <= ut) & 
                (pl.col("suspiciousness") >= st)
            ).height
            results.append((ut, st, filtered_count))
            print(f"Unique ≤ {ut:.1f}, Suspicious ≥ {st:.2f}: {filtered_count} points")
    
    # Find combination with reasonable number of points (not too few, not too many)
    # target_range = (50, 500)  # Adjust as needed
    # good_combinations = [(ut, st, count) for ut, st, count in results 
    #                     if target_range[0] <= count <= target_range[1]]
    
    # if good_combinations:
    #     print(f"\nRecommended threshold combinations (yielding {target_range[0]}-{target_range[1]} points):")
    #     for ut, st, count in good_combinations:
    #         print(f"  Unique ≤ {ut:.1f}, Suspicious ≥ {st:.2f}: {count} points")
    # else:
    #     print(f"\nNo combinations yielded {target_range[0]}-{target_range[1]} points.")
    #     print("Consider adjusting target range or thresholds.")

# Example usage:
if __name__ == "__main__":
    # script_analyze_hallucination()
    # script_analyze_duplication()

    # First explore threshold combinations
    explore_threshold_combinations()
    
    # Then create the 3D plot with chosen thresholds
    # Adjust these thresholds based on the exploration results
    results = script_analyze_combined_thresholds(
        unique_threshold=0.3, 
        suspicious_threshold=0.3,
        unique_range=(0.0, 0.4),
        suspicious_range=(0.0, 1.0)
    )