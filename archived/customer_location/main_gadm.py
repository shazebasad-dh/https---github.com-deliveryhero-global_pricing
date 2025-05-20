import os
# Set the current working directory to ~/global_pricing/customer_location
if "customer_location" in os.getcwd():
    pass
else:
    os.chdir(f"{os.getcwd()}/customer_location")
from gadm_scraper import download_gadm_datasets
from gadm_dataset_uploader import gadm_dataset_uploader

# Define a function that runs the two functions above
def main():
    # Download the GADM datasets
    download_gadm_datasets(
        base_url_var="https://gadm.org/download_country.html", # GADM website where we will download the shape files
        downloads_dir_var=f"C:/Users/{os.getenv('USERNAME')}/Downloads", # Default downloads directory on Windows. You might need to change this if you are using a Mac or Linux,
        final_downloads_dst_var=f"{os.getcwd()}/shape_files", # The final destination of the downloaded shape files,
        is_headless=False # Set to True if you want to run the script in headless mode
    )

    # Generate the GADM datasets
    gadm_dataset_uploader()

if __name__ == "__main__":
    main()