import logging
import os
from typing import Union
import pandas as pd
from google.cloud import bigquery
from itertools import combinations
import polars as pl
import numpy as np
from scipy import stats
import pingouin as pg
from tqdm import tqdm


########################################### LOGGER CLASS METRICS ###########################################
class Logger:
    def __init__(self, logger_name:str) -> None:
        # set logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        #stream handler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        # add handlers to logger
        self.logger.addHandler(stream_handler)
    
    def info(self, message):
        self.logger.info(message)

########################################### CONTINUOUS METRICS ###########################################

class ContinuousMetrics:
    DELIVERY_FEE = "delivery_fee_local"
    GFV = "gfv_local"
    TRAVEL_TIME = "travel_time"
    DELIVERY_DISTANCE = "delivery_distance"
    FLEET_DELAY = "fleet_delay"
    DELIVERY_COSTS = "delivery_costs_local"
    REVENUE = "revenue_local"
    PROFIT = "profit_local"
    COMMISSION = "commission_local"

    @classmethod
    def return_continuous_metrics(cls):
        return [
            cls.DELIVERY_FEE,
            cls.GFV,
            cls.TRAVEL_TIME,
            cls.DELIVERY_DISTANCE,
            cls.FLEET_DELAY,
            cls.DELIVERY_COSTS,
            cls.REVENUE,
            cls.PROFIT,
            cls.COMMISSION,
        ]
    
class TestConstants:
    CONTROL="Control"
    VARIATION1="Variation1"
    DEFAULT_TREATMENT_LEVELS = ["All", "True"]
    TTEST = "Welch's T-Test"
    ANOVA = "OneWay ANOVA"

class MENAEntities:
    HS_SA = "HS_SA"
    HF_EG = "HF_EG"
    TB_AE = "TB_AE"
    TB_OM = "TB_OM"
    TB_QA = "TB_QA"
    TB_KW = "TB_KW"
    TB_JO = "TB_JO"
    TB_IQ = "TB_IQ"
    TB_BH = "TB_BH"

    @classmethod
    def return_talabat_entities(cls):
        return [
            cls.HF_EG,
            cls.TB_AE,
            cls.TB_OM,
            cls.TB_QA,
            cls.TB_KW,
            cls.TB_JO,
            cls.TB_IQ,
            cls.TB_BH,
        ]



########################################### CLASS STORE ###########################################

class Store:
    current_test_data:pd.DataFrame = None
    last_metric_significance_run: list[dict] = None
    last_test_at_treatment_level_significance_run: list[dict] = None
    last_test_significance_run: list[dict] = None 
    results_dict: list[dict] = None

########################################### CONNECTOR ###########################################
class Connector:
    """
    Class purpose is to connect to BQ and run queries
    """
    billing_project_id: str = "logistics-data-staging-flat"
    pricing_project_id:str = "dh-logistics-product-ops"
    pricing_dataset_id: str = "pricing"

    
    def __init__(self, json_credentials:str) -> None:
        self.json_credentials = json_credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.json_credentials
        self.client = bigquery.Client(project = self.billing_project_id)
        self.logger = Logger("Connector")


    def load_table_to_bq_from_dataframe(self, table_id: str, df: pd.DataFrame) -> None:
        """Create a table in BigQuery from a pandas dataframe

        Args:
            table_id (str): id of the table to be created
            df (pd.DataFrame): dataframe to be loaded into BigQuery
        """
        job = self.client.load_table_from_dataframe(
            dataframe = df,
            destination= f'{self.pricing_project_id}.{self.pricing_dataset_id}.{table_id}',
            job_config = self._create_job_config()
        )
        job.result()

    def run_query(self, query: str) -> bigquery.job.QueryJob:
        """Run a query in BigQuery

        Args:
            query (str): query to run

        Returns:
            bigquery.job.QueryJob:
        """
        job = self.client.query(query)
        job.result()
        self.logger.info("Query ran successfully")
        return job

    def get_df_from_query(self, query: str, dataframe_type="Pandas") -> Union[pl.DataFrame, pd.DataFrame]:
        """Generate a dataframe from the results of a query

        Args:
            query (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        job = self.run_query(query)
        if dataframe_type == "Pandas":
            return (
                job.to_dataframe(progress_bar_type="tqdm")
            )
        return (
            pl.from_arrow(job.to_arrow(progress_bar_type="tqdm"))
        )
    
    # def get_df_from_query(self, query: str) -> pl.DataFrame:
    #     """Generate a dataframe from the results of a query

    #     Args:
    #         query (str): _description_

    #     Returns:
    #         pd.DataFrame: _description_
    #     """
    #     job = self.run_query(query)
    #     return (
    #         pl.from_arrow(job.to_arrow(progress_bar_type="tqdm"))
    #     )

    @staticmethod
    def _create_job_config() -> bigquery.LoadJobConfig:
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig()
        job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        return job_config
    

    def upload_table_to_dataframe(self, table_id: str, df: pd.DataFrame) -> None:
        """Load a table from a pandas dataframe

        Args:
            table_id (str): _description_
            df (pd.DataFrame): _description_
        """
        job_config: bigquery.LoadJobConfig = self._create_job_config()

        job = self.client.load_table_from_dataframe(
            dataframe=df,
            destination=f'{self.pricing_project_id}.{self.pricing_dataset_id}.{table_id}',
            job_config=job_config,
        )

        job.result()
        self.logger.info(f'Loaded {job.output_rows} rows into {self.pricing_project_id}.{self.pricing_dataset_id}:{table_id}.')
    

########################################### PRE-SIGNIFICANCE RUNNER #########################################

class PreSignificanceRunner:
    """
    This class runs the query in BQ to create the tables that functions as output
    to the Significance Class that runs the statistical tests
    """

    def __init__(self, connector: Connector, folder_path:str):
        self.connector = connector
        self.folder_path = folder_path
        self.logger = Logger("PreSignificanceRunner")

    def run_query_from_file(self, query_file:str):
        """Run a single query that's within
        the folder path

        Args:
            query_file (str): _description_
        """
        with open(f"{self.folder_path}/{query_file}", "r") as f:
            query = f.read()
        job = self.connector.run_query(query)
        self.logger.info(f"Query {query_file} ran successfully")
    
    def run_presteps(self):
        """Run all queries that are in the
        folder path
        """
        self.logger.info("Running pre-steps for significance tests")
        files_in_folders = os.listdir(self.folder_path)
        for file in files_in_folders:
            self.run_query_from_file(file)
        self.logger.info("PreSteps ran successfully")

########################################### SIGNIFICANCE RUNNER ###########################################
class SignificanceRunner:
    base_query:str = """
    SELECT *
    FROM `{0}`
    """
    entity_filter ='WHERE entity_id = "{0}"'
    test_name_filter = 'AND test_name = "{0}"'

    default_treatment_levels = ["All", "True"]

    def __init__(self, connector:Connector, input_table_id:str, output_table_id:str):
        self.connector = connector
        self.input_table_id = input_table_id
        self.output_table_id = output_table_id
        self.logger = Logger("SignificanceRunner")
        # self.store = Store()

    def _build_query(self, entity_id:str = None, test_name:str = None) -> str:
        """Returns the query to be run in BQ. By default it loads
        all orders from a given entity_id unless a test_name is provided in which
        it will only returns orders from that test.

        Args:
            entity_id (str): _description_
            test_name (str, optional): _description_. Defaults to None.

        Returns:
            str: _description_
        """
        self.query = self.base_query.format(
            f"{self.connector.pricing_project_id}.{self.connector.pricing_dataset_id}.{self.input_table_id}"
        )

        if entity_id is not None:
            self.query += self.entity_filter.format(entity_id)
            if test_name is not None:
                self.query += self.test_name_filter.format(test_name)
        return self.query

    def import_df(self, entity_id:str = None, test_name:str = None, dataframe_type:str="Pandas") -> Union[pl.DataFrame, pd.DataFrame]:
        """A function that queries the input data for the tests
        """
        # Run the query and return the output df:
        query = self._build_query(entity_id, test_name)
        df = self.connector.get_df_from_query(query, dataframe_type)
        return df
    
    ############## FILTERING FUNCTIONS ##############

    def detect_outliers(self, data:pd.Series, q=0.01):
        ''' The function takes a pandas series and a quantile between 0 and 0.1 and returns a boolean that equals true
            where the value is out of bound, otherwise false.
        '''
        if q > 0.1 or q < 0:
            raise TypeError('percentile must be between 0 and 10')
        lower_limit = data.quantile(q)
        upper_limit = data.quantile(1 - q)
        # self.logger.info(f'Lower limit: {lower_limit} Upper limit: {upper_limit}')
        clean_data = ~data.between(lower_limit, upper_limit)
        return clean_data
    
    def filter_outliers(self, df:pd.DataFrame, column:str, q=0.01):
        ''' The function takes a pandas dataframe and a column name and returns the dataframe without the outliers
        '''
        outliers = (df
                    .groupby('variant')
                    [column]
                    .transform(self.detect_outliers, q)
        )
        # outliers = self.detect_outliers(df[column], q)
        return df[~outliers]
        # return outliers
    
    def filter_data_to_test(self, df: pd.DataFrame, test_name:str) -> pd.DataFrame:
        return (
            df
            .loc[df["test_name"] == test_name]
        )

    def filter_data_to_treatment_group(self, df:pd.DataFrame, treatment:str):
        """
        The function filters the dataframe based on the treatment analysis.
        If "All" it returns all the data. 
        If "True" it returns the data where the treatment is True.
        If "Target Group X" it returns the data where the target_group is X.
        """
        if treatment != "True" and treatment != "All":
            test_target_group_list = self.get_target_group_list_from_test(df)
            if treatment not in test_target_group_list:
                raise TypeError("The treatment group does not exist in the test")
            return (
                df
                .loc[lambda df: df["target_group"] == treatment]
            )
        elif treatment == "True":
            return (
                df
                .loc[lambda df: df.treatment == True]
            )
        return df
    

    def explode_test_values(self, df:pd.DataFrame, current_metric:str) -> pd.DataFrame:
        """The function takes a dataframe and a metric and returns a dataframe with the exploded values
        """
        return (
            df
            [["test_name", "variant", current_metric]]
            .explode(current_metric)
            .astype({current_metric: "float64"})
        )
    
    def filter_data_to_entity(self, df:pd.DataFrame, entity_id:str) -> pd.DataFrame:
        return (
            df
            .loc[lambda df: df["entity_id"] == entity_id]
        )
    
    def filter_dataframe_for_metric(
        self, 
        df:pd.DataFrame, 
        test_name:str, 
        treatment:str,
        metric:str
        ) -> pd.DataFrame:
        
        return (df
                .pipe(self.filter_data_to_test, test_name)
                .pipe(self.filter_data_to_treatment_group, treatment)
                .pipe(self.explode_test_values, metric)
                .pipe(self.filter_outliers, metric, q=0.01)
                )
    
    def get_entities_in_dataframe(self, df:pd.DataFrame) -> list:
        return(
            df["entity_id"].unique().tolist()
        )
    

    
    ############## STATISTICAL FUNCTIONS ##############
    
    def t_test(self, c_array:np.array, v_array:np.array) -> float:
        """The function performs the Welch's t-test on the control and variation arrays.
            It returns the p-value.
        """
        stat, p_value = stats.ttest_ind(c_array, v_array, equal_var=False, nan_policy='omit')
        return p_value
    
    def split_dataset_into_variants(self, df:pd.DataFrame, column:str, variant_a:str, variant_b:str) -> tuple[np.array, np.array]:
        """The function splits the passed column into two passed arrays : variant_a and variant_b
        """
        variant_a = df[df['variant'] == variant_a][column].to_numpy()
        variant_b = df[df['variant'] == variant_b][column].to_numpy()
        return variant_a, variant_b
    
    def get_mean_and_count_by_variant(self, df:pd.DataFrame, metric:str) -> pd.DataFrame:
        """The function takes a dataframe and a metric and returns a dataframe with the mean and count by variant
        """
        return (
            df
            .groupby(["test_name", "variant"], as_index=False)
            .agg(
                mean=(metric, "mean"),
                count=(metric, "count"),
            )
        )
    
    def get_target_group_list_from_test(self, df:pd.DataFrame) -> list[str]:
        return ( df
                ["target_group"]
                .dropna()
                .unique()
                .tolist()
            )
    
    def get_number_of_variants_per_test(self, df:pd.DataFrame) -> list[str]:
        return ( df
                ["n_variants_in_test"]
                .iloc[0]
        )
    
    def get_number_of_iterations_to_run(self, n_variants_in_test:int, treatment_levels:list[str]) -> int:
        """Return the number of maximum iterations to run for a given test.
        For a test with 2 variants, the number of iterations is the number of metrics * number of treatment levels.
        For a test with more than 2 variants, 
        the number of iterations is the number of metrics * number of treatment levels * number of pairwise comparisons.
        Args:
            n_variants_in_test (int): _description_
            treatment_levels (list[str]): _description_

        Returns:
            int: _description_
        """
        if n_variants_in_test == 2:
            n_iter = len(ContinuousMetrics.return_continuous_metrics()) * len(treatment_levels)
        else:
            n_iter = len(list(combinations(range(n_variants_in_test), 2))) * len(ContinuousMetrics.return_continuous_metrics()) * len(treatment_levels)
        return n_iter
    
    def get_list_of_treatments_levels(self, raw_df:pd.DataFrame, test_name:str,) -> list[str]:
        """Return a list of treatment levels for a given test."""
        treatment_levels = []
        treatment_levels.append(TestConstants.DEFAULT_TREATMENT_LEVELS)
        treatment_levels.append(raw_df
            .pipe(self.filter_data_to_test, test_name)
            .pipe(self.get_target_group_list_from_test)
        )
        flat_treatment_levels = flatten_list(treatment_levels)
        return flat_treatment_levels

    
     
    ############## T-TEST ##############
    def apply_ttest_on_single_metric(
            self,
            current_test_data:pd.DataFrame, 
            mean_and_count_vals:pd.DataFrame,
            metric:str
        ) -> dict:

        """Run a TTest on a single metric.
        The function takes the raw dataframe loaded from BigQuery. 
        It first filters the data to the test_name, treatment and metric.
        Only then, it explodes the metric values into a long format and calculates
        the significance of that metrics. The result is a dictionary with the results of the
        t-test.
        
        It's intentional the design of having a single metric as input and load everything
        from the raw dataframe. This is to always have one variable that contains the long
        format of the data and optimize memory usage.

        Args:
            raw_df (pd.DataFrame): _description_
            test_name (str): _description_
            treament (str): _description_
            metric (str): _description_

        Returns:
            dict: _description_
        """
        result_dict = {}
        # run the t-test
        control, variation = self.split_dataset_into_variants(
            current_test_data, metric,
            TestConstants.CONTROL, 
            TestConstants.VARIATION1
        )
        p_value = self.t_test(control, variation)

        # fill the result dictionary
        result_dict["variant_a"] = TestConstants.CONTROL
        result_dict["variant_b"] = TestConstants.VARIATION1
        result_dict["kpi_label"] = metric
        result_dict["anova_p_value"] = None
        result_dict["p_value"] = p_value
        result_dict["mean_a"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.CONTROL, "mean"].values[0]
        result_dict["mean_b"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.VARIATION1, "mean"].values[0]
        result_dict["mean_a"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.CONTROL, "mean"].values[0]
        result_dict["mean_b"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.VARIATION1, "mean"].values[0]
        result_dict["values_count_a"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.CONTROL, "count"].values[0]
        result_dict["values_count_b"] = mean_and_count_vals.loc[mean_and_count_vals["variant"] == TestConstants.VARIATION1, "count"].values[0]
        return result_dict

    ############## ANOVA ##############

    def apply_anova_on_single_metric(
            self,
            current_test_data:pd.DataFrame,
            mean_and_counts:pd.DataFrame,
            metric:str
    ) -> list[dict]:
        # run the anova
        anova_result = pg.welch_anova(data=current_test_data, dv=metric, between="variant")["p-unc"][0]
        post_hoc_result = (
            pg.pairwise_gameshowell(data=current_test_data, dv=metric, between="variant", effsize="hedges")
            .assign(p_val_bonferroni=lambda df: pg.multicomp(df["pval"].values, alpha=0.05, method="bonf")[1])
            [["A", "B", "p_val_bonferroni"]]
        )
        # fill the result dictionary
        results = (
            post_hoc_result
            .merge(mean_and_counts, left_on=["A"], right_on=["variant"])
            .merge(mean_and_counts, left_on=["B"], right_on=["variant"], suffixes=("_a", "_b"))
            [["A", "B", "mean_a", "mean_b", "p_val_bonferroni", "count_a", "count_b"]]
            .rename(columns={"A": "variant_a", "B": "variant_b", "count_a": "values_count_a", "count_b": "values_count_b", "p_val_bonferroni": "p_value"})
            .to_dict(orient="records")
        )
        return results, anova_result

    
    ############## ORCHESTRATOR FUNCTIONS ##############

    def run_significance_on_single_metric(
            self,
            raw_df:pd.DataFrame,
            test_name:str,
            treatment:str,
            metric:str,
            n_variants_per_test:int
    ) -> list[dict]:
        """
        This function runs a significance test on a single metric based on the filter criteria
        and the number of variants per test:
        - 2 variants: t-test
        - more than 2 variants: ANOVA + Games-Howell post-hoc test
        It returns a list of dictionaries with the results of the test.
        """

        # filter the dataframe for the test and calculate the mean and count by variant.
        current_test_data = self.filter_dataframe_for_metric(raw_df, test_name, treatment, metric)
        mean_and_count_vals = self.get_mean_and_count_by_variant(current_test_data, metric)

        if n_variants_per_test == 2:
            # self.logger.info(f"Running t-test on {metric} for {test_name} and {treatment}")
            results = self.apply_ttest_on_single_metric(current_test_data, mean_and_count_vals, metric)
            results["test_name"] = test_name
            results["treatment"] = treatment
            results["statistical_method"] = TestConstants.TTEST
            results = [results]
        else:
            # self.logger.info(f"Running ANOVA on {metric} for {test_name} and {treatment}")
            results, anova_p_value = self.apply_anova_on_single_metric(current_test_data, mean_and_count_vals, metric)
            for single_dict in results:
                single_dict["treatment"] = treatment
                single_dict["kpi_label"] = metric
                single_dict["test_name"] = test_name
                single_dict["statistical_method"] = TestConstants.ANOVA
                single_dict["anova_p_value"] = anova_p_value
        # self.store.last_metric_significance_run = results
        return results
    
    def run_significance_on_all_metrics(
            self,
            raw_df:pd.DataFrame,
            test_name:str,
            treatment:str,
            n_variants_per_test:int,
    ) -> list[dict]:
        """This function runs a T-Test on all continuous metrics.
        for the passed test_name and treatment.

        Args:
            raw_df (pd.DataFrame): _description_
            test_name (str): _description_
            treatment (str): _description_

        Returns:
            _type_: _description_
        """
        result_list = []
        for metric in ContinuousMetrics.return_continuous_metrics():
            try:
                result_dict = self.run_significance_on_single_metric(raw_df, test_name, treatment, metric, n_variants_per_test)
                result_list.append(result_dict)
            except Exception as e:
                self.logger.info(f"Error in {test_name} at {treatment} for {metric}")
                self.logger.info(e)
        # self.store.last_test_at_treatment_level_significance_run = result_list
        return result_list
    
    
    def run_significance_on_test(
            self,
            raw_df:pd.DataFrame,
            test_name:str,
    ) -> list[dict]:
        
        flat_treatment_levels = self.get_list_of_treatments_levels(raw_df, test_name)
        # self.logger.info(f"Running {test_name} at {flat_treatment_levels}")

        n_varians_in_test = (raw_df
            .pipe(self.filter_data_to_test, test_name)
            .pipe(self.get_number_of_variants_per_test)
        )

        number_of_iterations = self.get_number_of_iterations_to_run(n_varians_in_test, flat_treatment_levels)
        self.logger.info(f"Max number of {number_of_iterations} iterations for {test_name}")

        result_list = []
        for treatment_i in tqdm(flat_treatment_levels, position=2, leave=False, desc=f"Running {test_name}"):
            try:
                result_list += self.run_significance_on_all_metrics(raw_df, test_name, treatment_i, n_varians_in_test)
            except:
                self.logger.info(f"Error in {test_name} at {treatment_i}")
                # self.logger.info(e)
        # self.store.last_test_significance_run = result_list
        return result_list
    
    def get_tests_in_entity(self, raw_df:pd.DataFrame) -> list[str]:
        """Returns a list of all tests in the entity"""
        return (
            raw_df
            ["test_name"]
            .unique()
            .tolist()
        )
    
    def add_test_statuses(self, result_df:pd.DataFrame, raw_df:pd.DataFrame, ) -> pd.DataFrame:
        """Returns the status of the test. Either Running or Ended

        Args:
            raw_df (pd.DataFrame): _description_
            test_name (str): _description_

        Returns:
            str: _description_
        """

        test_status = (
            raw_df
            [["test_name", "status", "country_code"]]
            .drop_duplicates()
        )

        return (
            result_df
            .merge(test_status, on="test_name", how="left")
        )    
   
    def run_significance_on_entity(
        self, 
        raw_df:pd.DataFrame,
        entity_id:str
    ) -> pd.DataFrame:
        tests_in_entity = (raw_df
                           .pipe(self.filter_data_to_entity, entity_id)
                           .pipe(self.get_tests_in_entity)
        )
        result_list = []
        for test_i in tqdm(tests_in_entity, position=1, desc=f"Running {entity_id}", leave=False):
            try:
                result_i = self.run_significance_on_test(raw_df, test_i)
                result_list.append(flatten_list(result_i))
            except:
                self.logger.info(f"Error in {test_i}")
                # self.logger.info(e)
        # self.store.entity_result = flatten_list(result_list)
        result_df = (
            pd.DataFrame(flatten_list(result_list))
            .pipe(self.add_test_statuses, raw_df)
        )
        return result_df  
    

########################################### GENERAL UTILS ###########################################

def flatten_list(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist]