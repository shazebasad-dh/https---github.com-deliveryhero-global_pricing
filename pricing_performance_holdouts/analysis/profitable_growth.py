import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def compute_profitable_growth(
    profit_result: dict,
    order_result: dict,
    alpha: float = 0.05
) -> dict:
    
    """
    Compute profitable growth and confidence interval from bootstrapped profit and order results,
    and return the observed profitable growth from the observed differences.

    Profitable growth is defined as:
        ((Δ profit / profit per order) + Δ orders) / (baseline holdout orders scaled by user ratio)

    Args:
        profit_result (dict): Output from bootstrap_diff_means_parallel for profit.
        order_result (dict): Output from bootstrap_diff_means_parallel for orders.
        alpha (float): Significance level for CI (default 0.05 for 95% CI).

    Returns:
        dict: Profitable growth point estimate (mean of bootstraps), confidence interval, 
              observed profitable growth (from raw diff), and full bootstrap distribution.
    """
    
    boot_profit = np.array(profit_result["boot_diffs"])
    boot_order = np.array(order_result["boot_diffs"])

    total_profit_non_holdout = profit_result["total_non_holdout"]
    total_orders_non_holdout = order_result["total_non_holdout"]

    total_profit_holdout = profit_result["total_holdout"]
    total_orders_holdout = order_result["total_holdout"]
    
    profit_per_order = total_profit_non_holdout / total_orders_non_holdout if total_orders_non_holdout != 0 else np.nan

    n_users_non_holdout = profit_result["n_users_non_holdout"]
    n_users_holdout = profit_result["n_users_holdout"]
    
    user_ratio = n_users_non_holdout / n_users_holdout
    baseline_orders = order_result["mean_holdout"] * (user_ratio * n_users_holdout)

    # Bootstrap distribution
    result = []
    B = len(boot_profit)
    for i in range(B):
        delta_profit = boot_profit[i]
        delta_order = boot_order[i]

        # Adjust profit_per_order if both values are negative
        adjusted_profit_per_order_bootstrap = (
            abs(profit_per_order)
            if (delta_profit * n_users_non_holdout < 0 and profit_per_order < 0)
            else profit_per_order
        )

        growth = (
            ((delta_profit * n_users_non_holdout) / adjusted_profit_per_order_bootstrap) +
            (delta_order * n_users_non_holdout)
        ) / baseline_orders

        result.append(growth)

    result = np.array(result)
    ci = np.nanpercentile(result, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    mean_val = np.nanmean(result)

    # Observed difference values
    observed_delta_profit = profit_result["observed_diff"]
    observed_delta_order = order_result["observed_diff"]

    # Adjust profit_per_order if both values are negative
    adjusted_profit_per_order_observed = (
        abs(profit_per_order)
        if (observed_delta_profit * n_users_non_holdout < 0 and profit_per_order < 0)
        else profit_per_order
    )

    observed_growth = (
        ((observed_delta_profit * n_users_non_holdout) / adjusted_profit_per_order_observed) +
        (observed_delta_order * n_users_non_holdout)
    ) / baseline_orders

    return {
        "profitable_growth": round(mean_val, 4),
        "observed_profitable_growth": round(observed_growth, 4),
        "ci": (round(ci[0], 4), round(ci[1], 4)),
        "distribution": result
    }


def apply_profitable_growth(
    boot_dic: dict,
    group_key_name: str = "entity"
) -> pd.DataFrame:
    """
    Summarize profitable growth metrics for each (group, week) pair in bootstrapped results.

    Args:
        boot_dic (dict): Dictionary of {(group, week): metrics_dict}, where metrics_dict
                         includes 'orders_post_cuped' and 'analytical_profit_post_cuped'.
        group_key_name (str): Name to use for the grouping key (e.g., 'entity', 'platform').

    Returns:
        pd.DataFrame: One row per (group, week) with profitable growth summary.
    """
    growth_rows = []

    for (group_key, week), metrics_dict in boot_dic.items():
        
        logger.info(f"Processing group: {group_key}, week: {week}")

        if "orders_post_cuped" not in metrics_dict or "analytical_profit_post_cuped" not in metrics_dict:
            continue  # skip if required keys missing

        order_result = metrics_dict["orders_post_cuped"]
        profit_result = metrics_dict["analytical_profit_post_cuped"]

        growth_summary = compute_profitable_growth(
        profit_result=profit_result,
        order_result=order_result
        )

        growth_rows.append({
            group_key_name: group_key,
            "week": week,
            "profitable_growth": growth_summary["profitable_growth"],
            "observed_profitable_growth": growth_summary["observed_profitable_growth"],
            "ci_low": growth_summary["ci"][0],
            "ci_high": growth_summary["ci"][1]
        })

    return pd.DataFrame(growth_rows)