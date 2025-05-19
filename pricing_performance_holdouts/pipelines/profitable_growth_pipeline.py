import pandas as pd
import numpy as np
import logging
from pricing_performance_holdouts.data.extract_stored_data import load_local_weekly_parquet_files
from pricing_performance_holdouts.data.store import store_profitable_growth
from pricing_performance_holdouts.analysis.bootstrap import apply_bootstrap_diff_means
from pricing_performance_holdouts.analysis.profitable_growth import apply_profitable_growth


logger = logging.getLogger(__name__)

def store_data_profitable_growth(weeks: list[str], vertical_type: str, group: str) -> list[tuple[str, pd.DataFrame]]:
    
    """
    Process weekly parquet files, apply bootstrap, and return results per week.

    Args:
        weeks: List of week strings, e.g., ['2025-05-01', '2025-05-08']
        vertical_type: Directory name under 'outputs/raw_data/' (e.g., 'restaurants')
        group: Grouping column (e.g., 'brand_name')

    Returns:
        List of tuples (week, result_df)
    """
    
    results = []

    for week in weeks:
        
        raw_df = load_local_weekly_parquet_files(
            week,
            f"outputs/raw_data/{vertical_type}"
        )

        order_profit_df, boot_dic = apply_bootstrap_diff_means(
            raw_df,
            group,
            ["orders_post_cuped", "analytical_profit_post_cuped"]
        )

        profitable_df = apply_profitable_growth(
            boot_dic,
            group
        )

        merged_df = pd.merge(
            order_profit_df,
            profitable_df,
            on=group,
            how='inner'
        )

        results.append((week, merged_df))


    store_profitable_growth(
    results=results,
    vertical_type=vertical_type,
    group=group,
    save_local=True,
    save_cloud_storage=False
    )

    return results

