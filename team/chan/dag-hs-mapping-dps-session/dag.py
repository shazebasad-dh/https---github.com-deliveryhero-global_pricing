from queries import *
from operators import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--days_back", "-d", type=int, default=14)

if __name__=="__main__":

    args = parser.parse_args()

    # init operator
    hs_transfer_operator = HSTransferOperator(
        project_id = "logistics-data-staging-flat"
        , credentials = "/Users/s.lafaurie/.config/gcloud/application_default_credentials.json"
        , dest_project_id = "dh-logistics-product-ops"
        , dataset_id = "pricing"
        , env="LOCAL"
    )

    # create staging data
    hs_transfer_operator.run_dag(
                staging_query = STAGING_QUERY
            , merge_query = MERGE_QUERY
            , qdd_query=QDD_QUERY
            , staging_table_name = "hs_sa_rdf_orders_stg"
            , production_table_name = "hs_sa_rdf_orders"
            , end_date = datetime.today()
            , days_back = args.days_back
    )
