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
   
    for week in tqdm(week_dates, desc= 'Week', position=0):
    
        GCS_PARQUET_PATH = f"parquet_files/cuped_holdout_as_of_{week}.parquet" 
        
        df_week = df[df['as_of_date'] == week]

        if save_local:
            output_dir = Path(__file__).resolve().parent.parent / "outputs" / "raw_data"
            output_dir.mkdir(parents=True, exist_ok=True)

            local_file = output_dir / f"cuped_holdout_as_of_{week}.parquet"
            df_week.to_parquet(local_file, engine="fastparquet", index=False)

        if save_cloud_storage:
            upload_parquet_to_gcs(df_week, bucket, GCS_PARQUET_PATH, overwrite=True)


