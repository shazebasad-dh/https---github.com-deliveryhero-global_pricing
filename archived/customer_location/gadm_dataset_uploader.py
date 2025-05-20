# Import packages
import logging
import os
import re

import geopandas as gpd
import pandas as pd
from google.cloud import bigquery, bigquery_storage

logging.basicConfig(
    filename="gadm.log",
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d-%m-%y %H:%M:%S",
    level=logging.INFO,
)
import warnings

warnings.filterwarnings(action="ignore")

###---------------------------------###---------------------------------###

def gadm_dataset_uploader():
    # Print a status message indicating that the GADM dataset uploader started
    logging.info("\nStarting the GADM dataset uploader...")

    # Set the current working directory to ~/global_pricing/customer_location/shape_files
    logging.info("Setting the current working directory to ~/global_pricing/customer_location/shape_files...")
    if "shape_files" in os.getcwd():
        pass
    else:
        try:
            os.chdir(f"{os.getcwd()}/customer_location/shape_files") # If the current working directory is ~/global_pricing
        except FileNotFoundError:
            os.chdir(f"{os.getcwd()}/shape_files") # If the current working directory is ~/global_pricing/customer_location

    ###---------------------------------###---------------------------------###

    # Instantiate a BigQuery client
    client = bigquery.Client(project="logistics-customer-staging")
    bqstorage_client = bigquery_storage.BigQueryReadClient()

    ###---------------------------------###---------------------------------###

    # Extract the three letter iso code of all countries in the global_entities table
    logging.info("Extracting the three letter iso code of all countries in the global_entities table...")
    country_name_query = """
        SELECT DISTINCT country_name, country_iso_a3
        FROM `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
        WHERE is_reporting_enabled = True
        ORDER BY 1
    """
    df_country_name = pd.DataFrame(client.query(query=country_name_query).result().to_dataframe(progress_bar_type="tqdm", bqstorage_client=bqstorage_client))

    # Update the country names of Vietnam in "df_country_name"
    df_country_name.loc[df_country_name["country_name"] == "Viet Nam", "country_name"] = "Vietnam"

    ###---------------------------------###---------------------------------###

    # Iterate over each country in df_country_name
    logging.info("Iterating over each country in df_country_name and creating separate data frames with a suffix indicating the level...")
    for iso in df_country_name["country_iso_a3"]:
        # List all the shape files that end with ".shp" and contain the 3-letter country code
        shp_files = [i for i in os.listdir(os.getcwd()) if i.endswith(".shp") and iso in i]

        # Loop over all levels of the shape file and create separate data frames with a suffix indicating the level (i.e., 0, 1, 2, etc). The highest level is 3
        for idx, shp in enumerate(shp_files):
            vars()[f"df_gpd_{iso.lower()}_" + str(idx)] = gpd.read_file(os.getcwd() + "/" + shp)


    ###---------------------------------###---------------------------------###

    # Declare a variable with the first five elements of the list containing the names of all the data frames that were created in the last step
    df_gpd_list = [i for i in dir() if i.startswith("df_gpd_")]

    ###---------------------------------###---------------------------------###

    # Merge the data frames that have common levels together
    logging.info("Merging the data frames that have common levels together...")

    # Declare empty lists to store the data frames that will be merged
    # First, create a list of all geo spatial levels in df_gpd_list
    geo_spatial_levels = pd.Series([i[-1] for i in df_gpd_list if i[-1].isnumeric()]).drop_duplicates().to_list()
    for i in geo_spatial_levels:
        vars()["df_gpd_" + i] = [] # This will create lists in the format df_gpd_0 = [], df_gpd_1 = [], df_gpd_2 = [], etc.

    # Iterate over the raw list of data frames and append them to the lists created above
    for i in df_gpd_list:
        for j in geo_spatial_levels:
            if i[-1] == j: # If the suffix of the data frame matches the suffix of the list, append the data frame to the list
                vars()["df_gpd_" + j].append(vars()[i])
            else:
                pass

    # Concat all the data frames in each list
    for i in geo_spatial_levels:
        vars()["df_gpd_" + i] = pd.concat(vars()["df_gpd_" + i]) # This is equivalent to df_gpd_0 = pd.concat(df_gpd_0), df_gpd_1 = pd.concat(df_gpd_1), etc.
        vars()["df_gpd_" + i] = vars()["df_gpd_" + i].reset_index(drop=True)
        vars()["df_gpd_" + i]["gid_level"] = i # Create a column that indicates the level of the geo spatial data frame

    ###---------------------------------###---------------------------------###

    # Define a job config to upload data to BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Upload the combined data frames to BigQuery
    df_gpd_merged_list = [i for i in dir() if re.findall(pattern="df_gpd_[0-9]", string=i)]
    for i in df_gpd_merged_list:
        logging.info(f"Uploading {i} to BigQuery...")

        # Change the column names to lower case
        vars()[i].columns = vars()[i].columns.str.lower()

        # Change the geometry column to a string
        vars()[i]["geometry"] = vars()[i]["geometry"].apply(lambda x: str(x))

        # Upload the data frames to BigQuery
        client.load_table_from_dataframe(
            dataframe=vars()[i],
            destination=f"logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_{i[-1]}",
            job_config=job_config
        ).result()