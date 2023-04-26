from src.utils import *
import pandas as pd
import tqdm
import warnings
import os 

warnings.filterwarnings("ignore")

def run_significance_on_talabat(
    runner: SignificanceRunner,
    entities_list: list[str] = MENAEntities.return_talabat_entities()
) -> pd.DataFrame:
    """Run significance on Talabat entities

    Args:
        runner (SignificanceRunner): _description_
        entities_list (list[str], optional): _description_. Defaults to MENAEntities.return_talabat_entities().

    Returns:
        pd.DataFrame: _description_
    """
    results_dataframe_list = []
    for entity_i in tqdm.tqdm(entities_list, desc=f"Running All Talabat entities", position=0):
        try:
            current_entity_df = runner.import_df(entity_i)
            result_i = runner.run_significance_on_entity(current_entity_df, entity_i)
            results_dataframe_list.append(result_i)
        except Exception as e:
            runner.logger.info(f"Error in {entity_i}")
            runner.logger.info(e)
    runner.logger.info(f"Finished running significance on Talabat")
    return pd.concat(results_dataframe_list, ignore_index=True)



def load_significance_results_to_biquery(
    runner: SignificanceRunner,
    results: pd.DataFrame
):
    """Load significance results to BigQuery"""
    runner.logger.info(f"Loading results to BigQuery")
    runner.connector.load_table_to_bq_from_dataframe(
        table_id = runner.output_table_id,
        df = results
    )
    runner.logger.info(f"Finished loading results to BigQuery")


def main(run_presteps=True):
    """Run significance on Talabat entities. 
    First run the presteps, then run significance on Talabat entities. Lastly, load the results to BigQuery
    """

    entities = MENAEntities.return_talabat_entities()
    # entities = ["TB_QA"]


    connector = Connector(
    json_credentials=open("/Users/s.lafaurie/Documents/pricing-local/MENA/tg-significance/.env").read().split("=")[1]
    )

    presteps = PreSignificanceRunner(
        connector
        , "queries"
    )

    significance_runner = SignificanceRunner(
        connector
        , input_table_id= "_dps_ab_test_significance_orders"
        , output_table_id= "_sl_dps_ab_test_significance_orders_results"
    )

    if run_presteps:
        presteps.run_presteps()

    results = run_significance_on_talabat(
        significance_runner,
        entities
    )

    load_significance_results_to_biquery(
        significance_runner,
        results
    )

if __name__ == "__main__":
    main()
