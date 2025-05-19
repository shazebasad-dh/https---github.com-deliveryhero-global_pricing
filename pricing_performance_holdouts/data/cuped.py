import pandas as pd
import numpy as np
import logging
from typing import List, Tuple
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Apply CUPED
# ------------------------------------------------------------------------------

def cuped_adjustment(df: pd.DataFrame,
                         pre_metric: str,
                         post_metric: str) -> pd.DataFrame:
    """
    Apply CUPED variance reduction to a single pre/post metric pair.

    Args:
        df (pd.DataFrame): Input DataFrame containing pre and post metric columns.
        pre_metric (str): Name of pre-period covariate column.
        post_metric (str): Name of post-period target column.

    Returns:
        pd.DataFrame: DataFrame with new column added: '{post_metric}_cuped'.
    """   
    
    df = df.copy() 
    mask = df[pre_metric] > 0

    pre = df.loc[mask, pre_metric]
    post = df.loc[mask, post_metric]

    if pre.var() == 0:
        df[post_metric + '_cuped'] = df[post_metric]
        return df

    theta = np.cov(pre, post)[0, 1] / pre.var()
    df.loc[mask, post_metric + '_cuped'] = post - theta * (pre - pre.mean())
    df[post_metric + '_cuped'] = df[post_metric + '_cuped'].fillna(df[post_metric])

    return df


def apply_cuped_adjustment(df: pd.DataFrame,
                           pre_post_metric_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Apply CUPED adjustment across entities, weeks, holdout groups, and multiple metrics.

    Args:
        df (pd.DataFrame): Input DataFrame containing:
            - entity_id
            - as_of_date
            - is_customer_holdout
            - metrics defined in pre_post_metric_pairs
        pre_post_metric_pairs (List[Tuple[str, str]]):
            List of tuples containing (pre_metric, post_metric) column names.

    Returns:
        pd.DataFrame: DataFrame with CUPED-adjusted columns added for each metric pair.
    """

    final_df = pd.DataFrame()

    entities = df['entity_id'].dropna().unique()
    weeks = df['as_of_date'].dropna().unique()

    entity_loop = tqdm(entities, desc='Entities', position=0)
    for entity_id in entity_loop:
        tmp_entity = df[df['entity_id'] == entity_id]

        week_loop = tqdm(weeks, desc='Weeks', position=1, leave=False)
        for as_of_date in week_loop:
            
            tmp_week = tmp_entity[tmp_entity['as_of_date'] == as_of_date]

            for holdout_flag in [True, False]:
                group_label = 'Holdout' if holdout_flag else 'Non-holdout'
                tmp_group = tmp_week[tmp_week['is_customer_holdout'] == holdout_flag]

                if tmp_group.empty:
                    continue

                logger.info(f"[{group_label}] Processing Entity: {entity_id}, Week: {as_of_date}")

                for pre_metric, post_metric in pre_post_metric_pairs:
                    try:
                        tmp_group = cuped_adjustment(tmp_group, pre_metric, post_metric)
                    except Exception as e:
                        logger.warning(f"⚠️ Error in CUPED for Entity {entity_id}, Week {as_of_date}: {e}")
                        continue
        
                final_df = pd.concat([final_df, tmp_group], ignore_index=True)
    
    logger.info(f"🎯 CUPED complete: Final DataFrame has {len(final_df)} rows.")

    return final_df