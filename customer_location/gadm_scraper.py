# Import packages
import os
import shutil
import time
import warnings
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

# If you get an error with the ChromeBrowser version, pip install chromedriver-binary and chromedriver-binary-auto from https://pypi.org/project/chromedriver-binary/
# Then run pip install --upgrade --force-reinstall chromedriver-binary-auto
# This will install redetect the required version and install the newest suitable chromedriver
# There is no need to use service=Service(executable_path=ChromeDriverManager().install()) anymore
import chromedriver_binary  # This will add the executable to your PATH so it will be found. You can also get the absolute filename of the binary with chromedriver_binary.chromedriver_filename
import pandas as pd
from google.cloud import bigquery, bigquery_storage
from joblib import Parallel, delayed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException

# Ignore warnings
warnings.filterwarnings(action="ignore")

###---------------------------------###---------------------------------###

def download_gadm_datasets(
        base_url_var="https://gadm.org/download_country.html", # GADM website where we will download the shape files
        downloads_dir_var=f"C:/Users/{os.getenv('USERNAME')}/Downloads", # Default downloads directory on Windows. You might need to change this if you are using a Mac or Linux,
        final_downloads_dst_var=f"{os.getcwd()}/shape_files", # The final destination of the downloaded shape files,
        is_headless=False # Set to True if you want to run the script in headless mode
    ):
    # Set the Chrome options
    chrome_options = Options()
    chrome_options.add_argument("start-maximized") # Required for a maximized Viewport
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation', 'disable-popup-blocking']) # Disable pop-ups to speed up browsing
    chrome_options.add_experimental_option("detach", True) # Keeps the Chrome window open after all the Selenium commands/operations are performed 
    chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en,en_US'}) # Operate Chrome using English as the main language
    if is_headless == True:
        chrome_options.add_argument("--headless=new") # Operate Selenium in headless mode
    chrome_options.add_argument('--no-sandbox') # Disables the sandbox for all process types that are normally sandboxed. Meant to be used as a browser-level switch for testing purposes only
    chrome_options.add_argument('--disable-gpu') # An additional Selenium setting for headless to work properly, although for newer Selenium versions, it's not needed anymore
    chrome_options.add_argument("enable-features=NetworkServiceInProcess") # Combats the renderer timeout problem
    chrome_options.add_argument("disable-features=NetworkService") # Combats the renderer timeout problem
    chrome_options.add_experimental_option('extensionLoadTimeout', 45000) # Fixes the problem of renderer timeout for a slow PC
    chrome_options.add_argument("--window-size=1920x1080") # Set the Chrome window size to 1920 x 1080

    ###---------------------------------###---------------------------------###

    # Global inputs
    base_url = base_url_var
    downloads_dir = downloads_dir_var
    final_downloads_dst = final_downloads_dst_var

    ###---------------------------------###---------------------------------###

    # Set the current working directory to ~/global_pricing/customer_location
    if "customer_location" in os.getcwd():
        pass
    else:
        os.chdir(f"{os.getcwd()}/customer_location")

    ###---------------------------------###---------------------------------###

    # Instantiate the BigQuery client
    client = bigquery.Client(project="logistics-customer-staging")
    bqstorage_client = bigquery_storage.BigQueryReadClient()

    # Pull the country names from the global_entities BigQuery table
    country_name_query = """
        SELECT DISTINCT country_name, country_iso_a3
        FROM `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
        WHERE is_reporting_enabled = True
        ORDER BY 1
    """
    df_country_name = client.query(query=country_name_query).result().to_dataframe(progress_bar_type="tqdm", bqstorage_client=bqstorage_client)

    # Update the country names of Vietnam in "df_country_name"
    df_country_name.loc[df_country_name["country_name"] == "Viet Nam", "country_name"] = "Vietnam"

    ###---------------------------------###---------------------------------###

    # Define a function to select a country and download its shape file
    def download_shape_file(country):
        # Pull the 3-letter country code from df_country_name
        country_code = df_country_name.loc[df_country_name["country_name"] == country, "country_iso_a3"].values[0]

        # Define the file name based on the country
        file_name = f"gadm41_{country_code}_shp.zip"

        # Instantiate the Webdriver and download the latest chrome driver by default using the ChromeDriverManager
        driver = webdriver.Chrome(service=Service(executable_path=ChromeDriverManager().install()), options=chrome_options)

        # Navigate to the target website
        driver.get(base_url)

        # Maximise the Chrome window
        driver.maximize_window()

        # Wait for the page to load and click on the country selector
        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//select[@id='countrySelect']")))
        try:
            select_country = Select(driver.find_element(by=By.XPATH, value="//select[@id='countrySelect']"))
            select_country.select_by_visible_text(country)
        except NoSuchElementException:
            print(f"Could not find the country {country} in the dropdown. Moving to the next country...\n")
            driver.quit()
            return

        # Download the shape file
        WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.XPATH, "//h6[@id='shp']/a")))
        driver.find_element(by=By.XPATH, value="//h6[@id='shp']/a").click()

        # Check if the file exists in the working directory
        path = Path(f"{downloads_dir}/{file_name}")
        t1 = datetime.now()

        # If the file doesn't exist, print a message saying that we are still waiting for the file to appear and wait 30 seconds before proceeding to the next command
        while path.is_file() == False:
            t2 = datetime.now()
            print(f"[{file_name} status] - Still waiting for the {file_name} to download. {t2 - t1} have elapsed thus far")
            time.sleep(10)

            # If the file exists, print a success message, close the driver, and move the file from the Downloads folder to the current directory
            if path.is_file() == True:
                print(f"[{file_name} status] - The file {file_name} has been downloaded. Closing the driver now...")

                # Close the driver
                driver.quit()

                # Move the file from the downloads folder to the current directory
                shutil.move(src=f"{downloads_dir}/{file_name}", dst=final_downloads_dst + f"/{file_name}")
                print(f"[{file_name} status] - Moved the file {file_name} to the current working directory. Moving to the next date...\n")
                
                # Break out of the loop
                break
        
        # Unzip the downloaded shape file
        with ZipFile(final_downloads_dst + f"/{file_name}", "r") as f:
            # Extract in current directory
            f.extractall(path=final_downloads_dst)
        
        # Delete the zip file
        os.remove(final_downloads_dst + f"/{file_name}")

        # Print a success message signifying that the process ended for this country
        print(f"The shape files of {country} have been successfully downloaded, moved, and unzipped. Moving to the next country...\n")
        return

    # Execute the function in parallel for all the countries in df_country_name
    Parallel(n_jobs=-1, verbose=10)(delayed(download_shape_file)(country=country) for country in df_country_name["country_name"].unique())