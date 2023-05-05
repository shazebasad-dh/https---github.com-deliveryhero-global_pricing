import os
import tempfile
from datetime import datetime, timedelta
import pyarrow as pa
import polars as pl
from google.cloud import bigquery


class HSTransferOperator:
    """
    Class contains the logic to run a DAG required to make HungerStation RDF data available in
    the pricing dataset
    """

    def __init__(
    self,
    env:str,
    project_id:str,
    dest_project_id:str,
    dataset_id:str,
    credentials:str = None
    ):
        self.env = env
        self.project = project_id
        self.dest_project_id = dest_project_id
        self.dataset_id = dataset_id
        self.credentials = credentials
        self.init_bq_client()

    def init_bq_client(self):
        if self.env == "LOCAL":
            self.credentials = self.credentials
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.credentials
            self.client = bigquery.Client(project=self.project)

        if self.env == "COLAB":
            from google.colab import auth, drive
            auth.authenticate_user()
            print('Authenticated')
            drive.mount('/content/gdrive')
            self.client = bigquery.Client(project=self.project)
            # set the working directory to the user gdrive
            os.chdir("/content/gdrive/MyDrive")

    def _load_job_config(self) -> bigquery.LoadJobConfig():
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.write_disposition = "WRITE_TRUNCATE"
        return job_config


    def _get_query_as_arrow_table(self, query:str) -> pa.Table:
        """Loads a Bigquery table dataframe into a Arrow Table.

        Args:
            query (str): The query to run

        Returns:
            (pa.Table)
        """
        return self.client.query(query).to_arrow(progress_bar_type="tqdm")

    def load_bigquery_into_polars(self, query:str) -> pl.DataFrame:
        """Loads a Bigquery table dataframe into a Polars DataFrame.

        Args:
            query (str): The query to run

        Returns:
            (pl.DataFrame)
        """
        arrow_data = self._get_query_as_arrow_table(query)
        df_polars = pl.from_arrow(arrow_data)
        del arrow_data
        return df_polars


    def load_polars_to_bigquery(
        self,
        dataframe: pl.DataFrame,
        job_config: bigquery.LoadJobConfig(),
        table_name:str
    ):
        """Loads a Polars dataframe to BigQuery.

        Args:
            project_id (str): The project ID for the BigQuery destination.
            dataset_id (str): The dataset ID for the BigQuery destination.
            table_name (str): The table name for the BigQuery destination.
            dataframe (pl.DataFrame): The Polars dataframe to load.

        Returns:
            None
        """

        #save local parquet
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            dataframe.write_parquet(temp_file.name)
            file_path = temp_file.name

        # set table name
        table_ref = f"{self.dest_project_id}.{self.dataset_id}.{table_name}"

        # Load the data into BigQuery
        with open(file_path, "rb") as temp_parquet:
            job = self.client.load_table_from_file(temp_parquet, table_ref, job_config= job_config)
            job.result()

        print(f"Loaded {len(dataframe)} rows to BigQuery table {table_name} in {self.dest_project_id}:{self.dataset_id}")

    def create_staging_data(self, query:str, table_name:str, end_date:datetime, days_back:str):
        """Operator to triggert the staging part of the DAG.
        This runs a query that fetch data from HS local table, save a temporary local copy and then
        load such copy to pricing dataset

        Args:
            query (str): query to load HS data
            table_name (str): name of the destination table
            end_date (datetime): run date of the task
            days_back (str): how many days back we want to fetch data
        """
        print("Initiating staging task...")
        query_with_dates = query.format(*self._return_job_dates(end_date, days_back))
        polars_df = self.load_bigquery_into_polars(query_with_dates)
        job_config = self._load_job_config()

        print("Loading from BigQuery into Polars successful")
        self.load_polars_to_bigquery(polars_df, job_config, table_name)

    def merge_into_prod(self, query:str, staging_table:str, prod_table:str):
        """Function that creates a BQ job that merge the staging data into production table.

        Args:
            query (str): Merge query statement
            staging_table (str): staging pricing HS table
            prod_table (str): production pricing HS table
        """
        print("Initiating merging task...")
        job = self.client.query(
            query.format(prod_table, staging_table)
        )
        job.result()
        print("Merge has finished")

    def run_qdd_query(self, query:str):
        """Creates a BQ job to run the query that produces the
        qdd table

        Args:
            query (str): QDD query
        """
        print("Initiating QDD task...")
        job = self.client.query(query)
        job.result()
        print("QDD has finished")

    def _return_job_dates(self, end_date:datetime, days_back:datetime) -> list[str]:
        """Return the run period  as list of string

        Args:
            end_date (datetime): run date of the task
            days_back (datetime): how many days back we want to fetch data

        Returns:
            list[str]: list of [start_date, end_date] used to fetch data
        """
        start_date = end_date - timedelta(days=days_back)
        return [end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d")]
    
    def run_dag(
        self
        , staging_query:str
        , merge_query:str
        , qdd_query:str
        , staging_table_name:str
        , production_table_name:str
        , end_date:datetime
        , days_back:int
    ):
        """Run whole DAG. Updates Pricing HS RDF data

        Args:
            staging_query (str): Query to create staging table
            merge_query (str): Query to merge staging into prod table
            staging_table_name (str): Staging table name
            production_table_name (str): Production table name
            end_date (datetime): run date of the task
            days_back (int): how many days back we want to fetch data
        """
        self.create_staging_data(
            staging_query
            , staging_table_name
            , end_date
            , days_back
        )

        self.merge_into_prod(
            merge_query
            , staging_table_name
            , production_table_name
        )


        self.run_qdd_query(
            qdd_query
        )

