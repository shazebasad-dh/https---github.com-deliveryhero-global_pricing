import numpy as np
import pandas as pd
from tqdm.auto import tqdm
from scipy.stats import t, ttest_ind
from typing import List, Dict, Union


def t_confidence_single_group(data: Union[pd.Series, np.ndarray], alpha: float = 0.05) -> Dict:
    
    """
    Computes mean and t-distribution confidence interval for a single group.

    Args:
        data (Union[pd.Series, np.ndarray]): Input numeric data.
        alpha (float): Significance level. Defaults to 0.05 for a 95% CI.

    Returns:
        Dict: Dictionary with mean and CI bounds.
    """

    data = np.array(data)
    n = len(data)
    mean = np.mean(data)
    sem = np.std(data, ddof=1) / np.sqrt(n)
    t_crit = t.ppf(1 - alpha / 2, df=n - 1)

    ci_lower = mean - t_crit * sem
    ci_upper = mean + t_crit * sem

    return {
        "mean": round(mean,3),
        "ci": (round(ci_lower,3), round(ci_upper,3))
    }


def welchs_ttest_with_ci(df: pd.DataFrame, adjusted_metric: str, alpha: float = 0.05) -> Dict:
    
    """
    Performs Welch's t-test between holdout and non-holdout groups for one metric.

    Args:
        df (pd.DataFrame): Input dataframe with 'is_customer_holdout' column.
        adjusted_metric (str): Column to compare.
        alpha (float): Significance level.

    Returns:
        Dict: Welch's test results including observed diff, CI, t-stat, p-value, etc.
    """    
    
    group_a = df[df["is_customer_holdout"] == False][adjusted_metric].values
    group_b = df[df["is_customer_holdout"] == True][adjusted_metric].values

    # Means and sizes
    mean_a, mean_b = np.mean(group_a), np.mean(group_b)
    n_a, n_b = len(group_a), len(group_b)
    var_a, var_b = np.var(group_a, ddof=1), np.var(group_b, ddof=1)

    # Welch's t-test
    t_stat, p_value = ttest_ind(group_a, group_b, equal_var=False)

    # Observed difference
    observed_diff = mean_a - mean_b

    # Standard error of the difference
    se_diff = np.sqrt(var_a / n_a + var_b / n_b)

    # Degrees of freedom (Welch-Satterthwaite equation)
    df_denom = (var_a / n_a + var_b / n_b) ** 2
    df_numer = ((var_a / n_a) ** 2) / (n_a - 1) + ((var_b / n_b) ** 2) / (n_b - 1)
    df_eff = df_denom / df_numer

    # t critical value
    t_crit = t.ppf(1 - alpha / 2, df=df_eff)

    # Confidence interval
    ci_lower = observed_diff - t_crit * se_diff
    ci_upper = observed_diff + t_crit * se_diff

    mean_holdout = np.mean(group_b)

    if mean_holdout != 0:
        pct_diff = observed_diff / abs(mean_holdout)
        pct_ci_low = ci_lower / abs(mean_holdout)
        pct_ci_high = ci_upper / abs(mean_holdout)
    else:
        pct_diff = pct_ci_low = pct_ci_high = np.nan

    return {
        "observed_diff": observed_diff,
        "t_statistic": t_stat,
        "p_value": p_value,
        "ci": (ci_lower, ci_upper),
        "se": se_diff,
        "pct_diff": pct_diff,
        "pct_ci": (pct_ci_low, pct_ci_high)
    }


def apply_welchs_diff_means(
    df: pd.DataFrame,
    group_col: str,
    adjusted_metrics: List[str],
    alpha: float = 0.05
) -> pd.DataFrame:
    
    """
    Applies Welch's t-test per week per group for multiple metrics.

    Args:
        df (pd.DataFrame): Input dataframe with 'as_of_date', 'is_customer_holdout', and metric columns.
        group_col (str): Column to group by (e.g. brand or entity).
        adjusted_metrics (List[str]): List of CUPED-adjusted metric names.
        alpha (float): Significance level for CI.

    Returns:
        pd.DataFrame: Summary of t-test stats per group per week.
    """
    
    results = []

    all_groups = df[group_col].dropna().unique()
    all_weeks = sorted(df["as_of_date"].dropna().unique())

    for group_val in tqdm(all_groups, desc=f"{group_col} groups"):
        df_group = df[df[group_col] == group_val]

        for week in tqdm(all_weeks, desc="Weeks", leave=False):
            df_cumulative = df_group[df_group["as_of_date"] == week]

            if df_cumulative["is_customer_holdout"].nunique() < 2:
                continue

            row = {
                group_col: group_val,
                "as_of_date": week
            }

            metric_summaries = []

            for metric in adjusted_metrics:
                if metric not in df_cumulative.columns:
                    continue

                try:
                    result = welchs_ttest_with_ci(
                        df=df_cumulative,
                        adjusted_metric=metric,
                        alpha=alpha
                    )

                    non_holdout = df_cumulative[df_cumulative["is_customer_holdout"] == False]
                    holdout = df_cumulative[df_cumulative["is_customer_holdout"] == True]

                    mean_non_holdout = non_holdout[metric].mean()
                    mean_holdout = holdout[metric].mean()
                    n_users_non_holdout = non_holdout.shape[0]
                    n_users_holdout = holdout.shape[0]

                    observed_diff = result["observed_diff"]
                    ci_low, ci_high = result["ci"]
                    pct_diff = result["pct_diff"]
                    pct_ci_low, pct_ci_high = result["pct_ci"]
                    
                    row[f"{metric}_observed_diff"] = observed_diff
                    row[f"{metric}_ci_low"] = ci_low
                    row[f"{metric}_ci_high"] = ci_high
                    
                    row[f"{metric}_pct_diff"] = pct_diff
                    row[f"{metric}_pct_ci_low"] = pct_ci_low
                    row[f"{metric}_pct_ci_high"] = pct_ci_high
                    
                    row[f"{metric}_se"] = result["se"]
                    row[f"{metric}_t_statistic"] = result["t_statistic"]
                    row[f"{metric}_p_value"] = result["p_value"]
                    row[f"{metric}_mean_non_holdout"] = mean_non_holdout
                    row[f"{metric}_mean_holdout"] = mean_holdout
                    row[f"{metric}_n_users_non_holdout"] = n_users_non_holdout
                    row[f"{metric}_n_users_holdout"] = n_users_holdout

                    metric_summaries.append(
                        f"{metric}: Diff={observed_diff:.4f}, CI=({ci_low}, {ci_high}), "
                        f"Users NHO={n_users_non_holdout}, HO={n_users_holdout}"
                    )

                except Exception as e:
                    print(f"Error with {group_col}={group_val}, week={week}, metric={metric}: {e}")
                    continue

            if metric_summaries:
                print(f"\nBrand/Entity: {group_val}")
                print(f"Week: {week.date()}")
                print("Results:")
                for summary in metric_summaries:
                    print(f"    {summary}")


            results.append(row)

    return pd.DataFrame(results)