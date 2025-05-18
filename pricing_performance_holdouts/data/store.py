import pandas as pd
import numpy as np
from google.cloud import storage
from tqdm.auto import tqdm
import io
import logging
from typing import List
from pathlib import Path


logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Data Storage Cloud Bucket
# ------------------------------------------------------------------------------


def upload_parquet_to_gcs(df: pd.DataFrame, 
                          bucket: storage.Bucket,
                          destination_blob: str,
                          overwrite: bool = True) -> None:
    
    """
    Upload a pandas DataFrame as a Parquet file to Google Cloud Storage.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        bucket (google.cloud.storage.Bucket): GCS bucket object.
        destination_blob (str): Path within bucket where file will be saved.
        overwrite (bool, optional): Whether to overwrite existing file. Defaults to True.

    Returns:
        None
    """
    
    parquet_buffer = io.BytesIO()
    
    try:
        # Check if the blob (file) already exists in GCS
        blob = bucket.blob(destination_blob)
        if blob.exists() and not overwrite:
            logger.info(f"⚠️ File already exists at {destination_blob}. Skipping upload due to overwrite=False.")
            return

        # Convert DataFrame to parquet and upload
        df.to_parquet(parquet_buffer, engine="fastparquet", index=False)
        parquet_buffer.seek(0)

        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")
        logger.info(f"✅ Data uploaded to GCS at {destination_blob} {'(overwritten)' if overwrite else ''}")

    except Exception as e:
        logger.error(f"❌ Error uploading Parquet to GCS: {e}")


def store_data_cloud(df: pd.DataFrame,
                     week_dates: List[pd.Timestamp],
                     vendor_vertical: tuple[str, ...],
                     gcs_bucket: str = "holdout_data",
                     project: str = "logistics-data-storage-staging",
                     save_local: bool = False,
                     save_cloud_storage: bool = False,
                     overwrite: bool = True) -> None:
    """
    Store a DataFrame as weekly Parquet files, optionally both locally and to GCS.

    The DataFrame is split by 'as_of_date' and each week's data is saved separately.

    Args:
        df (pd.DataFrame): DataFrame containing an 'as_of_date' column.
        week_dates (List[pd.Timestamp]): List of week start dates to filter on.
        gcs_bucket (str, optional): Name of GCS bucket. Defaults to "holdout_data".
        project (str, optional): GCP project ID. Defaults to "logistics-data-storage-staging".
        save_local (bool, optional): If True, also save each week's file locally as parquet. Defaults to False.
        save_cloud_storage (bool, optional): If True, upload files to GCS. Defaults to False.
        overwrite (bool, optional): If True, allows overwriting existing files in GCS. Defaults to True.

    Returns:
        None
    """
    
    #Configuration
    GCS_BUCKET_NAME = gcs_bucket
    PROJECT_ID = project

    #Initialize Google Cloud clients
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
   
    vertical_lower = [v.lower() for v in vendor_vertical]
    if any(v in vertical_lower for v in ['restaurant', 'restaurants']):
        folder_name = 'restaurants'
    else:
        folder_name = 'quick_commerce'
    
    for week in tqdm(week_dates, desc= 'Week', position=0):
        
        df_week = df[df['as_of_date'].dt.date == week]
        logger.info(f"Saving week {week} → {len(df_week)} rows")

        file_name = f"cuped_holdout_as_of_{week}.parquet"

        if save_local:
            output_dir = Path(__file__).resolve().parent.parent / "outputs" / "raw_data" / folder_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            local_file = output_dir / file_name
            df_week.to_parquet(local_file, engine="fastparquet", index=False)

        if save_cloud_storage:
            gcs_path = f"parquet_files/{folder_name}/{file_name}"
            upload_parquet_to_gcs(df_week, bucket, gcs_path, overwrite=overwrite)

def store_profitable_growth(
    results: list[tuple[str, pd.DataFrame]],
    vertical_type: str,
    group: str,
    save_local: bool = True,
    save_cloud_storage: bool = False,
    gcs_bucket: str = "holdout_data",
    project: str = "logistics-data-storage-staging",
    overwrite: bool = True
) -> None:
    """
    Store profitable growth results locally and/or to GCS.
    
    Args:
        results: List of (week, df) tuples
        vertical_type: 'restaurants' or 'quick_commerce'
        group: Grouping column used (e.g., 'brand_name' or 'entity_id')
        save_local: Save each DataFrame locally as a Parquet file
        save_cloud_storage: Upload each DataFrame to GCS
        gcs_bucket: GCS bucket name
        project: GCP project ID
        overwrite: If True, allows overwriting existing files in GCS
    """
    
    if save_cloud_storage:
        storage_client = storage.Client(project=project)
        bucket = storage_client.bucket(gcs_bucket)

    # Determine base output path based on group type
    if group == 'entity_id':
        base_folder = 'entity_profitable_growth'
    elif group == 'brand_name':
        base_folder = 'brand_profitable_growth'
    else:
        base_folder = 'other_profitable_growth'  # fallback for unknown groups

    for week, df in results:
        
        file_name = f"profitable_growth_{week}.parquet"

        if save_local:
            output_dir = Path(__file__).resolve().parent.parent / "outputs" / base_folder / vertical_type
            output_dir.mkdir(parents=True, exist_ok=True)
            local_path = output_dir / file_name
            df.to_parquet(local_path, index=False)
            logger.info(f"Saved local file → {local_path}")

        if save_cloud_storage:
            gcs_path = f"parquet_files/{base_folder}/{vertical_type}/{file_name}"
            blob = bucket.blob(gcs_path)

            if not overwrite and blob.exists():
                logger.warning(f"File {gcs_path} already exists in GCS. Skipping.")
                continue

            blob.upload_from_string(df.to_parquet(index=False), content_type='application/octet-stream')
            logger.info(f"Uploaded to GCS → {gcs_path}")


