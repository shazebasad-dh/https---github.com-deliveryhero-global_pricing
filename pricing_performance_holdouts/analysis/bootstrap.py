import pandas as pd
import numpy as np
from scipy import stats
from joblib import Parallel, delayed

# ------------------------------------------------------------------------------
# Performing Bootstrapping
# ------------------------------------------------------------------------------


def bootstrap_single_group(
    df: pd.DataFrame,
    adjusted_metric: str,
    n_resamples: int = 1000,
    alpha: float = 0.05,
    seed: int = 42
) -> dict:
    
    """
    Bootstrap mean and confidence interval for a single metric.

    Args:
        df (pd.DataFrame): DataFrame containing the metric column.
        adjusted_metric (str): Column to compute bootstrap over.
        n_resamples (int): Number of bootstrap resamples.
        alpha (float): Significance level (e.g., 0.05 for 95% CI).
        seed (int): Random seed for reproducibility.

    Returns:
        dict: Mean, CI, bootstrap mean, and bootstrap distribution.
    """

    group = df[adjusted_metric].dropna().values

    if len(group) == 0:
        raise ValueError(f"No valid data found for metric: {adjusted_metric}")

    if np.all(group == group[0]):
        mean_val = group[0]
        return {
            "mean": round(mean_val, 4),
            "ci": (round(mean_val, 4), round(mean_val, 4))
        }

    res = stats.bootstrap(
        (group,),
        statistic=np.mean,
        n_resamples=n_resamples,
        method='percentile',
        confidence_level=1 - alpha,
        random_state=seed
    )

    bootstrap_means = res.bootstrap_distribution
    ci = res.confidence_interval
    mean_val = np.mean(group)
    bootstrap_mean = np.mean(bootstrap_means)

    return {
        "mean": round(mean_val, 4),
        "ci": (round(ci.low, 4), round(ci.high, 4)),
        "bootstrap_mean": round(bootstrap_mean, 4),
        "bootstrap_means" : bootstrap_means
    }


def bootstrap_diff_means(
    df: pd.DataFrame,
    adjusted_metric: str,
    n_resamples: int = 300,
    alpha: float = 0.05,
    seed: int = 42,
    store_boot_diffs: bool = False,
):

    """
    Bootstrap difference in means between holdout and non-holdout groups.

    Args:
        df (pd.DataFrame): DataFrame containing 'is_customer_holdout' and metric.
        adjusted_metric (str): Name of metric column to analyze.
        n_resamples (int): Number of bootstrap samples.
        alpha (float): Significance level.
        seed (int): Random seed.
        store_boot_diffs (bool): If True, store bootstrapped diff distribution.

    Returns:
        dict: Dictionary with observed diff, CI, means, totals, and % diff.
    """

    rng = np.random.default_rng(seed)

    group_a = df[df["is_customer_holdout"] == False][adjusted_metric].values
    group_b = df[df["is_customer_holdout"] == True][adjusted_metric].values

    total_non_holdout = group_a.sum()
    total_holdout = group_b.sum()

    non_holdout = df[~df["is_customer_holdout"]]
    holdout = df[df["is_customer_holdout"]]
    n_users_non_holdout = non_holdout["customer_id"].nunique()
    n_users_holdout = holdout["customer_id"].nunique()

    mean_non_holdout = np.mean(group_a)
    mean_holdout = np.mean(group_b)

    res = stats.bootstrap(
        (group_a, group_b),
        statistic=lambda a, b: np.mean(a) - np.mean(b),
        n_resamples=n_resamples,
        method='percentile',
        random_state=rng
    )

    ci = res.confidence_interval
    observed_diff = mean_non_holdout - mean_holdout

    if np.isclose(mean_holdout, 0):
        pct_diff = observed_diff / abs(mean_holdout)
        pct_ci_low = ci.low / abs(mean_holdout)
        pct_ci_high = ci.high / abs(mean_holdout)
    else:
        pct_diff = pct_ci_low = pct_ci_high = np.nan

    bootstrap_ci_lb = round(ci.low, 4)
    bootstrap_ci_ub = round(ci.high, 4)
    bootstrap_mean = round(np.mean(res.bootstrap_distribution), 4)


    result = {
        "observed_diff": observed_diff,
        "bootstrap_mean": bootstrap_mean,
        "mean_non_holdout": mean_non_holdout,
        "mean_holdout": mean_holdout,
        "ci": (bootstrap_ci_lb, bootstrap_ci_ub),
        "pct_diff": pct_diff,
        "pct_ci": (round(pct_ci_low, 4), round(pct_ci_high, 4)),
        "total_non_holdout": total_non_holdout,
        "total_holdout": total_holdout,
        "n_users_non_holdout": n_users_non_holdout,
        "n_users_holdout": n_users_holdout
    }

    if store_boot_diffs:
        result["boot_diffs"] = res.bootstrap_distribution

    return result


def bootstrap_diff_means_parallel(
    df: pd.DataFrame,
    adjusted_metric: str,
    n_resamples: int = 1000,
    alpha: float = 0.05,
    seed: int = 42,
    store_boot_diffs: bool = False,
    n_jobs: int = -1,
):  

    """
    Parallelized bootstrap difference of means.

    Args:
        df (pd.DataFrame): DataFrame with 'is_customer_holdout' and metric.
        adjusted_metric (str): Metric to compare.
        n_resamples (int): Number of bootstrap samples.
        alpha (float): Significance level.
        seed (int): Seed for reproducibility.
        store_boot_diffs (bool): Store bootstrapped differences.
        n_jobs (int): Number of cores to use (-1 = all).

    Returns:
        dict: Similar to bootstrap_diff_means but faster for large datasets.
    """

    rng = np.random.default_rng(seed)

    group_a = df[~df["is_customer_holdout"]][adjusted_metric].values
    group_b = df[df["is_customer_holdout"]][adjusted_metric].values

    total_non_holdout = group_a.sum()
    total_holdout = group_b.sum()

    non_holdout = df[~df["is_customer_holdout"]]
    holdout = df[df["is_customer_holdout"]]
    n_users_non_holdout = non_holdout["customer_id"].nunique()
    n_users_holdout = holdout["customer_id"].nunique()

    mean_non_holdout = np.mean(group_a)
    mean_holdout = np.mean(group_b)
    observed_diff = mean_non_holdout - mean_holdout

    def single_resample(_):
        local_rng = np.random.default_rng(seed + _)

        resample_a = local_rng.choice(group_a, size=group_a.shape[0], replace=True)
        resample_b = local_rng.choice(group_b, size=group_b.shape[0], replace=True)

        return np.mean(resample_a) - np.mean(resample_b)

    diffs = Parallel(n_jobs=n_jobs)(
        delayed(single_resample)(i) for i in range(n_resamples)
    )

    diffs = np.array(diffs)
    ci_low = np.nanpercentile(diffs, 100 * (alpha / 2))
    ci_high = np.nanpercentile(diffs, 100 * (1 - alpha / 2))

    if not np.isclose(mean_holdout, 0):
        pct_diff = observed_diff / abs(mean_holdout)
        pct_ci_low = ci_low / abs(mean_holdout)
        pct_ci_high = ci_high / abs(mean_holdout)
    else:
        pct_diff = pct_ci_low = pct_ci_high = np.nan

    result = {
        "observed_diff": observed_diff,
        "bootstrap_mean": np.mean(diffs),
        "mean_non_holdout": mean_non_holdout,
        "mean_holdout": mean_holdout,
        "ci": (ci_low, ci_high),
        "pct_diff": pct_diff,
        "pct_ci": (pct_ci_low, pct_ci_high),
        "total_non_holdout": total_non_holdout,
        "total_holdout": total_holdout,
        "n_users_non_holdout": n_users_non_holdout,
        "n_users_holdout": n_users_holdout
    }

    if store_boot_diffs:
        result["boot_diffs"] = diffs

    return result


def apply_bootstrap_diff_means(
    df: pd.DataFrame,
    group_col: str,
    adjusted_metrics: list,
    n_resamples: int = 1000,
    alpha: float = 0.05,
    seed: int = 42
) -> pd.DataFrame:
    
    """
    Apply bootstrap difference-in-means for multiple metrics grouped by group_col and as_of_date.

    Args:
        df (pd.DataFrame): Input dataframe with CUPED-adjusted metrics and group/date info.
        group_col (str): Column name to group by (e.g., 'entity_id' or 'brand_name').
        adjusted_metrics (list): List of metric names to apply bootstrap diff means on.
        n_resamples (int): Number of bootstrap samples.
        alpha (float): Significance level for CI (default: 0.05).
        seed (int): Random seed for reproducibility.

    Returns:
        pd.DataFrame: A dataframe with bootstrap results for each group/date/metric.
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

            for metric in adjusted_metrics:
                if metric not in df_cumulative.columns:
                    continue

                try:
                    result = bootstrap_diff_means_parallel(
                        df=df_cumulative,
                        adjusted_metric=metric,
                        n_resamples=n_resamples,
                        alpha=alpha,
                        seed=seed
                    )

                    row[f"{metric}_observed_diff"] = result["observed_diff"]
                    row[f"{metric}_ci_low"] = result["ci"][0]
                    row[f"{metric}_ci_high"] = result["ci"][1]
                    
                    row[f"{metric}_abs_lift"] = result["observed_diff"] * result["n_users_non_holdout"]
                    row[f"{metric}_abs_ci_low"] = result["ci"][0] * result["n_users_non_holdout"]
                    row[f"{metric}_abs_ci_high"] = result["ci"][1] * result["n_users_non_holdout"]
                    
                    row[f"{metric}_pct_diff"] = result["pct_diff"]
                    row[f"{metric}_pct_ci_low"] = result["pct_ci"][0]
                    row[f"{metric}_pct_ci_high"] = result["pct_ci"][1]
                    
                    row[f"{metric}_total_non_holdout"] = result["total_non_holdout"]
                    row[f"{metric}_total_holdout"] = result["total_holdout"]
                                        
                    row[f"{metric}_mean_non_holdout"] = result["mean_non_holdout"]
                    row[f"{metric}_mean_holdout"] = result["mean_holdout"]
                    row[f"{metric}_n_users_non_holdout"] = result["n_users_non_holdout"]
                    row[f"{metric}_n_users_holdout"] = result["n_users_holdout"]
                    
                    #Optionally: remove or comment out if too large
                    # row[f"{metric}_boot_diffs"] = result["boot_diffs"]

                except Exception as e:
                    logger.info(f"Error with {group_col}={group_val}, week={week}, metric={metric}: {e}")
                    continue

            logger.info(f"\nBrand/Entity: {group_val}")
            logger.info(f"Week: {week.date()}")
            
            results.append(row)
            gc.collect()

    return pd.DataFrame(results)