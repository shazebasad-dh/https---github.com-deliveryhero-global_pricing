import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy.stats import norm

def plot_bootstrap_single_distribution(boot_result: dict, alpha: float = 0.05):
    
    """
    Plot bootstrap distribution for a single metric with confidence interval and mean.

    Args:
        boot_result (dict): Dictionary with keys 'bootstrap_means', 'mean', 'ci'.
        alpha (float): Significance level. Defaults to 0.05.
    """    
    
    boot_means = boot_result["bootstrap_means"]
    mean_val = boot_result["mean"]
    ci_low, ci_high = boot_result["ci"]

    plt.figure(figsize=(10, 6))
    plt.hist(boot_means, bins=50, edgecolor='black', alpha=0.7)
    plt.axvline(mean_val, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_val}')
    plt.axvline(ci_low, color='green', linestyle='--', linewidth=2, label=f'{100*(1-alpha):.0f}% CI Low: {ci_low}')
    plt.axvline(ci_high, color='green', linestyle='--', linewidth=2, label=f'{100*(1-alpha):.0f}% CI High: {ci_high}')
    
    plt.title("Bootstrap Distribution of the Mean")
    plt.xlabel("Mean Values")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_bootstrap_comparison_single_group(result1: dict, label1: str, result2: dict, label2: str, alpha: float = 0.05):
    
    """
    Compare two bootstrap distributions for single group metrics.

    Args:
        result1 (dict): First group's bootstrap result dictionary.
        label1 (str): Label for first group.
        result2 (dict): Second group's bootstrap result dictionary.
        label2 (str): Label for second group.
        alpha (float): Significance level. Defaults to 0.05.
    """
    
    plt.figure(figsize=(12, 6))

    # Plot 1
    plt.hist(result1["boot_means"], bins=50, alpha=0.5, label=f'{label1} Dist', edgecolor='black')
    plt.axvline(result1["mean"], color='red', linestyle='--', label=f'{label1} Mean: {result1["mean"]}')
    plt.axvline(result1["ci"][0], color='green', linestyle='--', label=f'{label1} CI Low: {result1["ci"][0]}')
    plt.axvline(result1["ci"][1], color='green', linestyle='--', label=f'{label1} CI High: {result1["ci"][1]}')

    # Plot 2
    plt.hist(result2["boot_means"], bins=50, alpha=0.5, label=f'{label2} Dist', edgecolor='black')
    plt.axvline(result2["mean"], color='blue', linestyle='--', label=f'{label2} Mean: {result2["mean"]}')
    plt.axvline(result2["ci"][0], color='purple', linestyle='--', label=f'{label2} CI Low: {result2["ci"][0]}')
    plt.axvline(result2["ci"][1], color='purple', linestyle='--', label=f'{label2} CI High: {result2["ci"][1]}')

    plt.title("Bootstrap Distribution Comparison")
    plt.xlabel("Mean Values")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_bootstrap_distribution(result_dict):
    
    """
    Plot bootstrap distribution of mean differences for two groups.

    Args:
        result_dict (dict): Result of bootstrap_diff_means with 'boot_diffs', 'observed_diff', 'ci'.
    """    
    
    diffs = result_dict["boot_diffs"]
    observed = result_dict["observed_diff"]
    ci_low, ci_high = result_dict["ci"]

    plt.figure(figsize=(10, 6))
    sns.histplot(diffs, bins=50, kde=True, color="skyblue")

    # Plot observed mean difference
    plt.axvline(observed, color="red", linestyle="solid", linewidth=2, label=f'Observed Mean Diff ({observed:.4f})')

    # Plot confidence interval bounds
    plt.axvline(ci_low, color="black", linestyle="dotted", linewidth=2, label=f'Lower CI ({ci_low:.4f})')
    plt.axvline(ci_high, color="black", linestyle="dotted", linewidth=2, label=f'Upper CI ({ci_high:.4f})')

    plt.title("Bootstrap Distribution of Mean Differences", fontsize=14)
    plt.xlabel("Mean Difference", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_bootstrap_comparison_differences_in_means(result_dict_1, result_dict_2, label_1="Group 1", label_2="Group 2"):
    
    """
    Compare two bootstrap distributions of mean differences.

    Args:
        result_dict_1 (dict): First result dict from bootstrap_diff_means.
        result_dict_2 (dict): Second result dict from bootstrap_diff_means.
        label_1 (str): Label for first group.
        label_2 (str): Label for second group.
    """    
    
    diffs_1 = result_dict_1["boot_diffs"]
    observed_1 = result_dict_1["observed_diff"]
    ci_low_1, ci_high_1 = result_dict_1["ci"]

    diffs_2 = result_dict_2["boot_diffs"]
    observed_2 = result_dict_2["observed_diff"]
    ci_low_2, ci_high_2 = result_dict_2["ci"]

    plt.figure(figsize=(12, 7))

    # Plot both distributions
    sns.histplot(diffs_1, bins=50, kde=True, color="skyblue", label=f'{label_1} Bootstrap', stat="density", alpha=0.6)
    sns.histplot(diffs_2, bins=50, kde=True, color="orange", label=f'{label_2} Bootstrap', stat="density", alpha=0.6)

    # Observed mean lines
    plt.axvline(observed_1, color="blue", linestyle="solid", linewidth=2, label=f'{label_1} Observed ({observed_1:.4f})')
    plt.axvline(observed_2, color="darkorange", linestyle="solid", linewidth=2, label=f'{label_2} Observed ({observed_2:.4f})')

    # CI bounds
    plt.axvline(ci_low_1, color="blue", linestyle="dotted", linewidth=1.5, label=f'{label_1} CI Low ({ci_low_1:.4f})')
    plt.axvline(ci_high_1, color="blue", linestyle="dotted", linewidth=1.5, label=f'{label_1} CI High ({ci_high_1:.4f})')

    plt.axvline(ci_low_2, color="darkorange", linestyle="dotted", linewidth=1.5, label=f'{label_2} CI Low ({ci_low_2:.4f})')
    plt.axvline(ci_high_2, color="darkorange", linestyle="dotted", linewidth=1.5, label=f'{label_2} CI High ({ci_high_2:.4f})')

    plt.title("Comparison of Bootstrap Distributions", fontsize=16)
    plt.xlabel("Mean Difference", fontsize=13)
    plt.ylabel("Density", fontsize=13)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_ttest_distribution(observed_diff, se_diff, ci, alpha=0.05, n_points=1000):
    
    """
    Plot the sampling distribution for Welch's t-test with CI and observed difference.

    Args:
        observed_diff (float): Observed difference in means.
        se_diff (float): Standard error of the difference.
        ci (tuple): Confidence interval.
        alpha (float): Significance level. Defaults to 0.05.
        n_points (int): Number of points for plotting the curve. Defaults to 1000.
    """    
    
    # Simulate normal distribution around observed_diff
    x = np.linspace(observed_diff - 4*se_diff, observed_diff + 4*se_diff, n_points)
    y = norm.pdf(x, loc=observed_diff, scale=se_diff)

    plt.figure(figsize=(10, 6))
    plt.plot(x, y, label="Sampling Distribution (Normal)", color='skyblue')
    
    # Observed difference
    plt.axvline(observed_diff, color='red', linestyle='-', label='Observed Mean Diff')
    plt.text(observed_diff, max(y)*0.9, f"{observed_diff:.4f}", color='red', ha='center', va='bottom', fontsize=10)

    # CI bounds
    plt.axvline(ci[0], color='black', linestyle='--', label=f'{100*(1-alpha):.0f}% CI')
    plt.axvline(ci[1], color='black', linestyle='--')
    plt.text(ci[0], max(y)*0.6, f"{ci[0]:.4f}", color='black', ha='right', va='bottom', fontsize=10)
    plt.text(ci[1], max(y)*0.6, f"{ci[1]:.4f}", color='black', ha='left', va='bottom', fontsize=10)

    # Labels and formatting
    plt.title("Sampling Distribution of Mean Difference (Welch’s t-test)")
    plt.xlabel("Mean Difference")
    plt.ylabel("Density")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()