import os 
from google.cloud import bigquery
import pandas as pd
import geopandas as gpd
from shapely import geometry, wkt




class Connector:
    """
    Class purpose is to handle connection to BQ and I/O operations
    """
    def __init__(self, credentials:str, project_id:str) -> None:
        self.credentials = credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials
        self.client = bigquery.Client(project = project_id)
        # self.logger = Logger("Connector")

    def run_query(self, query: str) -> bigquery.job.QueryJob:
        """Run a query in BigQuery

        Args:
            query (str): query to run

        Returns:
            bigquery.job.QueryJob:
        """
        job = self.client.query(query)
        job.result()
        return job

    def get_df_from_query(self, query: str) -> pd.DataFrame:
        """Generate a dataframe from the results of a query

        Args:
            query (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        job = self.run_query(query)
        return (
            job.to_dataframe(progress_bar_type="tqdm")
        )
    
    def load_from_table(self, table_id: str) -> pd.DataFrame:
        """Load a table from BQ directly into a dataframe

        Args:
            table_id (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        table = self.client.get_table(table_id)
        return self.client.list_rows(table).to_dataframe(progress_bar_type="tqdm")
    
def convert_to_geopandas(df:pd.DataFrame, wkt_column:str) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(df, geometry=df[wkt_column].apply(wkt.loads))

def row_number(data:pd.DataFrame, partition_cols:list[str], sort_col:str, col_name:str="row_number", ascending:bool=True):
    """
    SQL-like row_number function
    """
    return (
        data
        .assign(**{col_name: (
            data
            .sort_values(by= sort_col, ascending=True)
            .groupby(partition_cols)
            .cumcount() + 1
        )}
        )
    )