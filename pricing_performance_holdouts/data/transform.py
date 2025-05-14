import pandas as pd
import numpy as np
import logging
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Data Clean Up
# ------------------------------------------------------------------------------

def fill_nans(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaN values in orders and profit columns with zeros.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with NaNs filled.
    """

    df = df.fillna({
        'orders_pre': 0,
        'orders_post': 0,
        'analytical_profit_pre': 0,
        'analytical_profit_post': 0,
    })

    return df

def drop_users_with_no_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove users with no pre or post period orders and profit data.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    
    mask1 = (
        (df['orders_pre'] == 0) &
        (df['orders_post'] == 0) &
        (df['analytical_profit_pre'] == 0) &
        (df['analytical_profit_post'] == 0)
    )

    mask2 = (
        (df['orders_pre'].isna()) &
        (df['orders_post'].isna()) &
        (df['analytical_profit_pre'].isna()) &
        (df['analytical_profit_post'].isna())
    ) 

    df = df[~(mask1 |   mask2)]
    
    return df

def apply_cleanup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply NaN filling and remove users with no data in both periods.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """

    initial_rows = len(df)
    data_cleaned = fill_nans(df)
    raw_data_final = drop_users_with_no_data(data_cleaned)
    final_rows = len(raw_data_final)

    logger.info(f"apply_cleanup(): {initial_rows - final_rows} rows removed. Final dataset size: {final_rows}")

    return raw_data_final

def convert_dtypes(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    
    """
    Convert columns in DataFrame to specified data types.

    Args:
        df (pd.DataFrame): DataFrame to convert.
        dtype_map (dict): Dictionary of column names and target data types.

    Returns:
        pd.DataFrame: DataFrame with updated dtypes.
    """
    for col, dtype in dtype_map.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logging.warning(f"Could not convert column {col} to {dtype}: {e}")
    return df