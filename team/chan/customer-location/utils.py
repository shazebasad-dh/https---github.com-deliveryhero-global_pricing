import os 
from google.cloud import bigquery
import pandas as pd
import geopandas as gpd
from shapely import geometry, wkt
from dataclasses import dataclass
from scipy.stats import norm
import math
import seaborn as sns
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np



###################### Types And Classes

@dataclass
class QueryArgs:
    filename:str 
    params: dict[str, any]


@dataclass
class DeltaParameters:
    """Holds the parameters relevant to delta method variance.
    more info here: https://arxiv.org/abs/2305.16459

    As a general definition
    denom_mean -> Denominator mean, 
    demon_var -> Denominator Variance
    num_mean -> numerator mean
    num_var -> numerator variance
    covar -> covariance between numerator and denominator

    For example, sessions per user acts a denominator while transactions per user does it as numerator.
    """
    denom_mean:float
    num_mean:float 
    denom_var:float 
    num_var:float 
    covar:float 
    cvr:float 
    sample_size:float
    
    def calculate_variance_factor(self):
        """ h is the variable name in the paper source. 
        I view is as an adjusted variance. This is a separated
        function as this factor is useful for both the design and analysis phase. 

        Returns:
            _type_: _description_
        """
        num = self.num_var - 2*self.covar*self.num_mean/self.denom_mean + (self.num_mean**2)/(self.denom_mean**2)*self.denom_var
        den = self.denom_mean**2
        return num/den
    
    def calculate_sample_variance(self):
        """Full variance calculation requires sample size.
        Returns:
            _type_: _description_
        """
        return self.calculate_variance_factor() / self.sample_size
    

@dataclass
class PowerParams:
    """Class to hold default inputs for power analysis. 
    As a note, MDE must be ABSOLUTE
    """
    mde:float 
    alpha:float = 0.05
    power:float = 0.8
    n_variants:int=2

    def calculate_z_params(self) -> tuple:
        """Return the equivalent z-score
        of the alpha and power params.
        It assumes a two-tails test by default.

        Returns:
            _type_: _description_
        """
        z_alpha = norm.ppf(1-self.alpha/2)
        z_beta = norm.ppf(self.power)
        return (z_alpha, z_beta)

@dataclass
class MappingResults:
    final_mapping:dict
    is_feasible:bool
    mapping_logs:list[dict]
    power_params:PowerParams
    pop_share:float


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

class QueryHandler:
    folder = "queries"

    @classmethod
    def build_query(cls, query_args:QueryArgs) -> str:
        path = os.path.join(cls.folder, query_args.filename)
        with open(path, "r") as file:
            query = file.read().format(**query_args.params)
        return query 

##################### Helpers


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

def find_n_smallest_key(my_dict:dict, order:int=0) -> str:
    """Utility function that returns the key of the n-smallest value. 
    For example, if my_dict = {'A': 5, 'B': 1, 'C': 2, 'D': 10}.
    Calling:
        - find_n_smallest_area(my_dict, 0) returns "B"
        - find_n_smallest_area(my_dict, 1) returns "C"

    Args:
        areas_with_sample_size_difference (dict): _description_

    Returns:
        str: _description_
    """
    sort_values = sorted(my_dict.items(), key=lambda item: item[1])
    return sort_values[order][0]

##################### Delta method functions

def aggregate_ratio_data(df:pd.DataFrame, denom_col:str, num_col:str) -> DeltaParameters:
    """Take a dataframe that holds information at the randomization level (e.g user) and 
    calculate the variance/averages parameters used in the delta method.

    Args:
        df (pd.DataFrame): _description_
        session_col (str): _description_
        conversion_col (str): _description_

    Returns:
        DeltaParameters: _description_
    """
    params =  {
            "denom_mean": df[denom_col].mean()
            , "num_mean": df[num_col].mean()
            , "denom_var": df[denom_col].var()
            , "num_var":df[num_col].var()
            , "covar":df[denom_col].cov(df[num_col])
            , "cvr": df[num_col].sum() / df[denom_col].sum()
            , "sample_size": df.shape[0]
        }
    return DeltaParameters(**params)

def calculate_delta_method_sample_size(delta_params: DeltaParameters, power_inputs:PowerParams) -> float:
    """Calculate number of users (sample size) using the delta method.
    Method details are in this paper: https://arxiv.org/abs/2305.16459

    As a summary, the delta method uses a corrected formula to get
    the correct variance in the case sessions are correlated. This correlation usually happens
    experiment randomizes on users but CVR is on session level.

    Args:
        delta_params (DeltaParameters): _description_
        power_inputs (PowerInputs): _description_

    Returns:
        float: _description_
    """
    
    h = delta_params.calculate_variance_factor()
    z_alpha, z_power = power_inputs.calculate_z_params()
    k = math.ceil ( ( 2 * h * ( (z_alpha + z_power)**2)) / (power_inputs.mde ** 2) )
    return k

##################### Merging algorithm functions

def update_area_mappings(area_mapping:dict, current_area:str, next_area:str) -> dict[str, str]:
    """This function update the mapping between the area and the group they get added to.
    The idea of the mapping is to change the value of a given key. At the first iteration,
    key and value are the same, i.e, the area name. As iterations goes on an smaller areas
    get joined to other, the value will change to reference the area they're added to. 

    For example, at iteration 0 we got the following mapping {"A":"A", "B":"B", "C":"C"}.
    In the first iteration, it was decided that area "C" should be added to add, this function
    should return the following dictionary: {"A":"A", "B":"B", "C":"A"}.

    Args:
        area_mapping (dict): _description_
        current_area (str): _description_
        next_area (str): _description_

    Returns:
        dict[str, list[str]]: _description_
    """
    # replace the current key value for the next area
    area_mapping[current_area] = next_area
    return {key:val.replace(current_area, next_area) for key, val in area_mapping.items()}

def aggregate_user_data_per_area(df: pd.DataFrame, group_col:str) -> pd.DataFrame:
        """This functions aggregate the input data, which is on user level
        and calculate aggregated metrics at the area level

        Args:
            df (pd.DataFrame): _description_
            agg_col (str): _description_

        Returns:
            pd.DataFrame: _description_
        """
        return (
                df
                .groupby( [group_col], as_index=False)
                .agg(
                        avg_sessions = ("n_sessions", "mean")
                        ,  avg_conversions = ("n_conversions", "mean")
                        ,  total_session = ("n_sessions", "sum")
                        ,  total_conversion = ("n_conversions", "sum")
                        ,  n_users = ("perseus_client_id", "count")
                )
                .sort_values(by="n_users", ascending=True)
        )

def aggregate_shape_data_per_area(df:gpd.GeoDataFrame, area_mapping:dict[str,str], map_col:str) -> gpd.GeoDataFrame:
        """This function is used to obtain the combined polygon shape of areas that has been merged.
        The area_mapping argument keeps track of such mapping from the original/detailed areas to the 
        combined ones. The result is another geopandas dataframe but with only the combined areas names and shapes

        Args:
            df (gpd.GeoDataFrame): _description_
            area_mapping (dict[str,str]): _description_

        Returns:
            gpd.GeoDataFrame: _description_
        """
        return (
                df
                .assign(**{map_col:lambda df: df["area_name"].map(area_mapping)})
                .dissolve(by=map_col, aggfunc="sum")
                .reset_index()
        )

def get_neighbors_from_shapes(shapes_df:gpd.GeoDataFrame, area_df:pd.DataFrame) -> pd.DataFrame:
        """This functions calculate the smallest neighbor for each area. A neighbor is defined as two polygons
        sharing a common border. Smallest is defined in terms of number of users each area has.

        Args:
            shapes_df (gpd.GeoDataFrame): _description_
            area_df (pd.DataFrame): _description_

        Returns:
            pd.DataFrame: _description_
        """

        return (
                shapes_df
                .sjoin(shapes_df, how="left", predicate="touches")
                [["area_map_left", "area_map_right"]]
                .rename(columns={"area_map_left":"area_map", "area_map_right":"neighbor_name"})
                .merge(area_df, left_on="neighbor_name", right_on="area_map", suffixes=(None, "_y"))
                [["area_map", "neighbor_name", "n_users"]]
                # .pipe(row_number, partition_cols=["area_map"], sort_col="n_users", col_name="neighbor_rank")
        )

def run_customer_location_power_analysis(
        it_df: pd.DataFrame, 
        it_shapes: pd.DataFrame, 
        power_params:PowerParams, 
        pop_share:float=1.0, 
        n_it:int=100, 
        verbose:bool=False, 
        check_city=True, 
        show_charts:bool=False,
        use_progress_bar:bool=False
    ) -> MappingResults:
    
    """

    This 
    
    
    Improvements to do:
        - Parametrize columns used
        - Move the city check before running the loops --> DONE
        - Check if all areas meet sample size after the smallest one does it  --> DONE
        - set merging as optional
        - pass list of areas to NOT merge

    Args:
        it_df (pd.DataFrame): _description_
        it_shapes (pd.DataFrame): _description_
        power_params (PowerParams): _description_
        n_it (int, optional): _description_. Defaults to 100.

    Returns:
        dict: _description_
    """

    if check_city:
        city_delta_params =  it_df.pipe(aggregate_ratio_data, "n_sessions", "n_conversions")
        city_sample_size = calculate_delta_method_sample_size(city_delta_params, power_params)
        available_sample_size = math.floor(city_delta_params.sample_size * pop_share )

        if available_sample_size < city_sample_size:
            if verbose == True:
                print("City is NOT able to meet sample size")
                print(f"Current city size: {city_sample_size}. Available sample size {available_sample_size}")
                print("No need to run algoritm, returning empty mapping as default")
            return MappingResults({}, False, [], power_params,pop_share)


    area_mapping_logs = []
    area_mapping = {x:x for x in it_shapes.area_name.unique()}
    # area_mapping_logs += [area_mapping]
    # print(area_mapping_logs)

    iterations = min(it_shapes.shape[0], n_it)
    # print(f"Original number of areas {it_shapes.shape[0]}")
    
    iterations_range = tqdm(range(iterations)) if use_progress_bar == True else range(iterations)

    for i in iterations_range:
        # print(f"iteration {i}")

        # update data with current mapping
        it_df = (it_df 
                .assign(**{"area_map":lambda df: df["area_name"].map(area_mapping)})
        )

        agg_data = (
            it_df
            .pipe(aggregate_user_data_per_area, "area_map")
        )

        agg_shapes = (
            it_shapes
            .pipe(aggregate_shape_data_per_area, area_mapping, "area_map")
        )

        areas_with_delta_params = {x:aggregate_ratio_data(it_df.query("area_map==@x"), denom_col="n_sessions", num_col="n_conversions") for x in area_mapping.values()}
        areas_with_sample_size_difference = {key: math.ceil(val.sample_size * pop_share) - calculate_delta_method_sample_size(val, power_params) for key,val in areas_with_delta_params.items() }
        area_with_largest_difference = find_n_smallest_key(areas_with_sample_size_difference)

        # all must be higher than 0, otherwise there's one area that don't meet sample size
        if areas_with_sample_size_difference[area_with_largest_difference] > 0:
            if verbose == True:
                print("Sample size has been reached!")
                print(f"Largest difference is {areas_with_sample_size_difference[area_with_largest_difference]}")
                print(f"{len(set(area_mapping.values()))} unique areas")
                print()
            # i dont know why the first original mapping get lost. Short fix to make sure original mapping area preserved
            area_mapping_logs.insert(0, {x:x for x in it_shapes["area_name"].unique()})
            return MappingResults(area_mapping, True, area_mapping_logs, power_params,pop_share)

        # continue
        if verbose == True:
            print("Sample size not reached!")
            print(f"Largest difference is {areas_with_sample_size_difference[area_with_largest_difference]}")

        if agg_data.shape[0] == 1:
            if verbose == True:
                print("There's only one area, returning city statistics")
            area_mapping_logs.insert(0, {x:x for x in it_shapes["area_name"].unique()})
            return MappingResults(area_mapping, False, area_mapping_logs,power_params,pop_share)

        neighbors = (
                agg_shapes
                .pipe(get_neighbors_from_shapes, agg_data)
                .query(f"area_map=='{area_with_largest_difference}'")
                .neighbor_name
                .unique()
        )

        if len(neighbors) > 0:
            neighbors_differences = {key:val for key, val in areas_with_sample_size_difference.items() if key in neighbors}
            area_to_add = find_n_smallest_key(neighbors_differences)
        else:
            area_to_add = find_n_smallest_key(areas_with_sample_size_difference, 1)
            if verbose == True:
                print(f"""Area {area_with_largest_difference} have no neighbors. 
                    Adding it to the second area with largest diffference."""
                )

        if show_charts == True:
            fig, ax = plt.subplots(4,1, figsize=(15,8*4))
            ax = ax.flatten()

            # current shape
            agg_shapes.plot(column="area_map", edgecolor="black", ax=ax[0], legend=False, cmap="tab20")
            ax[0].set_axis_off()
            

            # sample size differences
            sns.barplot(
                pd.Series(areas_with_sample_size_difference).sort_values()
                , ax=ax[1]
            )

            for label in ax[1].get_xticklabels():
                label.set_fontsize(20)
                label.set_rotation(90)

            # neighbors + current area shape plot
            shapes_with_neighbors = (
                agg_shapes
                .assign(
                    neighbors = np.where(agg_shapes["area_map"].isin(neighbors),True, np.nan),
                    current_area = agg_shapes["area_map"] == area_with_largest_difference,
                    area_to_add = agg_shapes["area_map"] == area_to_add
                )
            )

            base = (
                shapes_with_neighbors
                .plot(
                    column="neighbors"
                    , missing_kwds={
                        "color":"lightgrey"
                    }
                    , ax=ax[2]
                )
            )

            (
                shapes_with_neighbors
                .query("current_area")
                .plot(
                    column="current_area"
                    , ax=ax[2]
                )
            )

            (
                shapes_with_neighbors
                .query("area_to_add")
                .plot(
                    column="area_to_add"
                    , color="red"
                    , ax=ax[2]
                )
            )
            ax[2].set_axis_off()

            # neighbors difference
            sns.barplot(
                pd.Series(neighbors_differences).sort_values()
                , ax=ax[3]
            )

            for label in ax[3].get_xticklabels():
                label.set_fontsize(20)
                label.set_rotation(90)



            fig.tight_layout()
            plt.show()

        if verbose == True:
            print(f"{area_with_largest_difference} to be merged with {area_to_add}")
            print(f"{len(set(area_mapping.values()))} unique areas remain")
            print()

                #plot current progress at each n iteration
            
        area_mapping = update_area_mappings(area_mapping, area_with_largest_difference, area_to_add)
        area_mapping_logs += [area_mapping]