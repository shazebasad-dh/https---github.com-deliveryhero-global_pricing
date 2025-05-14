from datetime import date, timedelta
import pandas as pd
import logging
from pricing_performance_holdouts.data.extract import initialize_bigquery_client, extract_data
from pricing_performance_holdouts.data.bigquery_queries import get_marketing_data, get_dps_data
from pricing_performance_holdouts.data.transform import apply_cleanup
from pricing_performance_holdouts.data.transform import convert_dtypes
from pricing_performance_holdouts.data.cuped import apply_cuped_adjustment
from pricing_performance_holdouts.data.store import store_data_cloud
from pricing_performance_holdouts.utils.dates import get_iso_week_mondays


logger = logging.getLogger(__name__)

def store_data_historically(project_id: str,
                            entities: list,
                            year: int = 2025,
                            min_date: date = None,
                            max_date: date = None,
                            restaurant_flag: str = 'IN',
                            pre_post_metric_pairs: list = [("orders_pre", "orders_post"),
                                                           ("analytical_profit_pre", "analytical_profit_post")],
                            save_local: bool = True) -> None:
    """
    Backfill pipeline to extract, clean, CUPED adjust and store weekly data.

    Args:
        project_id (str): GCP project ID.
        entities (list): Entity IDs to filter.
        year (int): Year to get all ISO weeks for.
        min_date (date, optional): Earliest date to include (Monday). Defaults to None.
        max_date (date, optional): Latest date to include (Monday). Defaults to latest Monday.
        restaurant_flag (str): 'IN' or 'NOT IN' for restaurant filtering.
        pre_post_metric_pairs (list): List of (pre, post) metric pairs for CUPED.
        save_local (bool): If True, save local parquet files. Defaults to True.

    Returns:
        None
    """

    client = initialize_bigquery_client(project_id)

    # If max_date is None, set to latest Monday
    if max_date is None:
        today = date.today()
        max_date = today - timedelta(days=today.weekday())

    week_mondays = get_iso_week_mondays(year, min_date=min_date, max_date=max_date)
    logger.info(f"Starting historical storage for {year} with {len(week_mondays)} weeks.")

    all_data = pd.DataFrame()

    for week in week_mondays:
        
        logger.info(f"Processing week: {week}")

        mkt_query = get_marketing_data(entities, week, restaurant_flag=restaurant_flag)
        dps_query = get_dps_data(entities, week, restaurant_flag=restaurant_flag)

        df_raw = extract_data(client, mkt_query, dps_query)
        df_raw["as_of_date"] = week

        df_clean = apply_cleanup(df_raw)

        # Convert dtypes for all pre & post columns
        dtype_map = {
            "orders_pre": int,
            "orders_post": int,
            "analytical_profit_pre": float,
            "analytical_profit_post": float,
            "as_of_date": "datetime64[ns]"
        }

        df_clean_dtypes = convert_dtypes(df_clean, dtype_map)
        
        all_data = pd.concat([all_data, df_clean_dtypes], ignore_index=True)

    df_cuped = apply_cuped_adjustment(all_data, pre_post_metric_pairs=pre_post_metric_pairs)

    store_data_cloud(
        df=df_cuped,
        week_dates=week_mondays,
        save_cloud_storage=False,
        save_local=save_local
    )

    logger.info("Historical storage complete.")

