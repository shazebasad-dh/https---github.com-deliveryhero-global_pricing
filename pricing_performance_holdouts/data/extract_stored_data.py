import pandas as pd
from pathlib import Path
from typing import Union, List, Optional

def load_local_weekly_parquet_files(
    weeks: Union[str, List[str], None] = None,
    folder_name: str = "outputs/raw_data"
) -> pd.DataFrame:
    
    """
    Load and concatenate locally saved weekly Parquet files.

    Args:
        weeks (Union[str, List[str], None]): 
            - None → loads all available files.
            - str → load single week (format: 'YYYY-MM-DD').
            - List[str] → list of weeks to load.
        folder_name (str): Directory containing the Parquet files.

    Returns:
        pd.DataFrame: Concatenated DataFrame of loaded files.
    """
    folder_path = Path(__file__).resolve().parent.parent / folder_name

    if not folder_path.exists():
        raise FileNotFoundError(f"Directory not found: {folder_path}")

    # Normalize input
    if isinstance(weeks, str):
        weeks = [weeks]

    if weeks is None:
        # Load all files
        files = sorted(folder_path.glob("cuped_holdout_as_of_*.parquet"))
    else:
        # Load specific files
        files = [
            folder_path / f"cuped_holdout_as_of_{week}.parquet"
            for week in weeks
        ]

    if not files:
        raise FileNotFoundError(f"No files found for the specified weeks in {folder_path}")

    df_list = []
    for file in files:
        if not file.exists():
            raise FileNotFoundError(f"File does not exist: {file}")
        df_list.append(pd.read_parquet(file))

    return pd.concat(df_list, ignore_index=True)


def load_local_profitable_growth_files(
    weeks: Union[str, List[str], None] = None,
    vertical_type: str = "restaurants",
    group: str = "entity_id",
    base_folder: str = "outputs"
) -> pd.DataFrame:
    """
    Load and concatenate locally saved profitable growth Parquet files.

    Args:
        weeks (Union[str, List[str], None]): 
            - None → loads all available files.
            - str → load single week (format: 'YYYY-MM-DD').
            - List[str] → list of weeks to load.
        vertical_type (str): 'restaurants' or 'quick_commerce'
        group (str): Grouping column, used to determine subfolder
        base_folder (str): Root folder where profitable growth files are stored

    Returns:
        pd.DataFrame: Concatenated DataFrame of loaded files
    """
    # Determine subfolder based on group
    if group == "entity_id":
        subfolder = "entity_profitable_growth"
    elif group == "brand_name":
        subfolder = "brand_profitable_growth"
    else:
        subfolder = "other_profitable_growth"

    folder_path = Path(__file__).resolve().parent.parent / base_folder / subfolder / vertical_type

    if not folder_path.exists():
        raise FileNotFoundError(f"Directory not found: {folder_path}")

    # Normalize input
    if isinstance(weeks, str):
        weeks = [weeks]

    if weeks is None:
        files = sorted(folder_path.glob("profitable_growth_*.parquet"))
    else:
        files = [
            folder_path / f"profitable_growth_{week}.parquet"
            for week in weeks
        ]

    if not files:
        raise FileNotFoundError(f"No files found for the specified weeks in {folder_path}")

    df_list = []
    for file in files:
        if not file.exists():
            raise FileNotFoundError(f"File does not exist: {file}")
        df_list.append(pd.read_parquet(file))

    return pd.concat(df_list, ignore_index=True)