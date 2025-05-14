from google.cloud import bigquery
import logging
import os
import json
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.auth import default
from datetime import date, timedelta
import pandas as pd
import numpy as np
import time
from typing import Optional, List

# ------------------------------------------------------------------------------
# Data Extraction
# ------------------------------------------------------------------------------

logger = logging.getLogger(__name__)

def initialize_bigquery_client(project_id_pass: str) -> bigquery.Client:

    """
    Initialize a BigQuery client for the given project.

    Args:
        project_id_pass (str): GCP project ID.

    Returns:
        bigquery.Client: Initialized BigQuery client object.
    """
    project_id = project_id_pass
    logger.info(f"Initializing BigQuery client for project: {project_id}")

    if os.getenv("GITHUB_ACTIONS"):
        credentials_path = "/tmp/credentials.json"
    else:
        credentials, project = default()
        project_id = project if project else project_id

    if os.getenv("GITHUB_ACTIONS") and not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Credentials file not found at {credentials_path}.")

    if os.getenv("GITHUB_ACTIONS"):
        with open(credentials_path, "r") as f:
            creds_data = json.load(f)

        credentials = Credentials.from_authorized_user_info(creds_data)

        if credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())

    try:
        client = bigquery.Client(credentials=credentials, project=project_id)
        logger.info(f"BigQuery client initialized for project: {project_id}")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise


def get_iso_week_mondays(year: int,
                         min_date: Optional[date] = None,
                         max_date: Optional[date] = None) -> List[date]:
    
    """
    Generate all Monday dates (start of ISO weeks) for a given year.

    Args:
        year (int): Year for which to get ISO week Mondays.
        min_date (Optional[date]): Minimum date to include.
        max_date (Optional[date]): Maximum date to include.

    Returns:
        List[date]: List of Monday dates.
    """

    d = date(year, 1, 4)
    d -= timedelta(days=d.weekday())
    mondays = []

    while d.year <= year or (d.year == year + 1 and d.isocalendar()[1] == 1):
        if (
            d.isocalendar()[0] == year and
            (min_date is None or d >= min_date) and
            (max_date is None or d <= max_date)
        ):
            mondays.append(d)
        d += timedelta(weeks=1)

    return mondays


def combined_data(client: bigquery.Client,
                  mkt: str,
                  dps: str) -> pd.DataFrame:
    """
    Run two BigQuery queries (marketing + DPS) and return combined DataFrame.

    Args:
        client (bigquery.Client): BigQuery client.
        mkt (str): Marketing query string.
        dps (str): DPS query string.

    Returns:
        pd.DataFrame: Appended DataFrame from both queries.
    """
    try:
        mkt_df = client.query(mkt).to_dataframe()
    except Exception as e:
        logger.error(f"Error running marketing query: {str(e)}")
        mkt_df = pd.DataFrame()

    try:
        dps_df = client.query(dps).to_dataframe()
    except Exception as e:
        logger.error(f"Error running DPS query: {str(e)}")
        dps_df = pd.DataFrame()

    return pd.concat([mkt_df, dps_df], ignore_index=True)


def extract_data(client: bigquery.Client,
                 mkt_data: str,
                 dps_data: str) -> pd.DataFrame:
    """
    Extract and combine marketing + DPS data.

    Args:
        client (bigquery.Client): BigQuery client.
        mkt_data (str): Marketing query.
        dps_data (str): DPS query.

    Returns:
        pd.DataFrame: Combined dataset.
    """
    start_time = time.time()
    logger.info("Starting data extraction...")

    combined_df = combined_data(client, mkt_data, dps_data)

    elapsed_time = time.time() - start_time
    logger.info(f"Data extraction completed in {elapsed_time:.2f} seconds.")

    return combined_df