import pandas as pd
from typing import List, Optional, Union

def summarize_columns(df: pd.DataFrame, 
                      columns: Optional[List[str]] = None,
                      groupby: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
    """
    Compute summary statistics for selected columns, optionally grouped by one or more columns.

    Args:
        df (pd.DataFrame): Input DataFrame.
        columns (list, optional): Columns to summarize. If None, summarize all numeric columns.
        groupby (str or list of str, optional): Column(s) to group by before summarizing.

    Returns:
        pd.DataFrame: Summary statistics table.
    """
    
    if columns is None:
        columns = df.select_dtypes(include='number').columns.tolist()

    if groupby is not None:
        grouped = df.groupby(groupby)
        results = []

        group_cols = [groupby] if isinstance(groupby, str) else groupby

        for group_keys, group_df in grouped:
            group_stats = summarize_columns(group_df, columns=columns, groupby=None)

            if isinstance(group_keys, tuple):
                for key_name, key_value in zip(group_cols, group_keys):
                    group_stats[key_name] = key_value
            else:
                group_stats[group_cols[0]] = group_keys

            results.append(group_stats)

        summary_df = pd.concat(results, ignore_index=True)
        summary_df = summary_df[[*group_cols, 'column', 'mean', 'std', 'min', 'max', 'missing_pct']]
        return summary_df

    summary = []
    for col in columns:
        summary.append({
            'column': col,
            'mean': df[col].mean(),
            'std': df[col].std(),
            'min': df[col].min(),
            'max': df[col].max(),
            'missing_pct': df[col].isna().mean() * 100
        })

    return pd.DataFrame(summary)