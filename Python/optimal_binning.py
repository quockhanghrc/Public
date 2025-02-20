import numpy as np
import pandas as pd
from itertools import combinations

## Define the optimal binning with highest IV - use for logistic regression

def calculate_iv(df, feature, target, bins):
    """Computes IV based on given bins and also returns the target rate in each bin."""
    # Ensure bin edges are unique before applying pd.cut
    bins = np.unique(bins)
    
    df['bin'] = pd.cut(df[feature], bins=bins, include_lowest=True)
    
    good = df[target].sum()
    bad = df.shape[0] - good
    
    grouped = df.groupby('bin')[target].agg(['sum', 'count'])
    grouped['good_dist'] = grouped['sum'] / good
    grouped['bad_dist'] = (grouped['count'] - grouped['sum']) / bad
    grouped['woe'] = np.log(grouped['good_dist'] / grouped['bad_dist'])
    grouped['iv'] = (grouped['good_dist'] - grouped['bad_dist']) * grouped['woe']
    
    # Calculate the target rate in each bin
    grouped['target_rate'] = grouped['sum'] / grouped['count']
    
    return grouped['iv'].sum(), grouped

def grid_search_optimal_bins(df, feature, target, num_bins=5, bin_multiplier=2,min_bin_pct=0.05):
    """Finds the optimal binning that maximizes IV using grid search, 
       with each bin containing at least min_bin_pct of total population."""
    
    # First, split the feature into num_bins * 2 bins
    n_bins = num_bins * bin_multiplier
    sorted_values = np.percentile(df[feature], np.linspace(0, 100, n_bins + 1)[1:-1])
    
    best_iv = -np.inf
    best_bins = None
    best_bin_pct = None
    best_target_rate = None
    
    # Generate all combinations of adjacent bins and try merging them into 5 bins
    for edges in combinations(range(n_bins), num_bins - 1):  # Choose (num_bins - 1) split points from n_bins
        # Define the bin edges by combining adjacent bin groups
        bin_edges = [-np.inf]  # Start with -inf as the first bin edge
        for i in range(1, n_bins):
            if i in edges:
                bin_edges.append(sorted_values[i - 1])  # Merge adjacent bins
        
        bin_edges.append(np.inf)  # End with +inf for the last bin
        
        # Ensure bin edges are unique
        bin_edges = np.unique(bin_edges)
        
        iv, grouped = calculate_iv(df, feature, target, bin_edges)
        
        # Check if each bin has at least min_bin_pct of the total population
        bin_sizes = grouped['count'] / df.shape[0]
        if (bin_sizes < min_bin_pct).any():  # Skip if any bin is too small
            continue
        
        # If this configuration gives a higher IV, update the best configuration
        if iv > best_iv:
            best_iv = iv
            best_bins = bin_edges
            best_bin_pct = bin_sizes  # Save the bin percentages
            best_target_rate = grouped['target_rate']  # Save the target rates
    
    return best_bins, best_iv, best_bin_pct, best_target_rate

# Example usage
df = pd.DataFrame({'feature': np.random.randn(1000), 'target': np.random.randint(0, 2, 1000)})
best_bins, max_iv, best_bin_pct, best_target_rate = grid_search_optimal_bins(df, 'feature', 'target', num_bins=5)

print(f"Best Bins: {best_bins}")
print(f"Maximum IV: {max_iv}")
print(f"Bin Percentages: {best_bin_pct}")
print(f"Target Rates per Bin: {best_target_rate}")
