import logging
from datetime import date
import sys
from pricing_performance_holdouts.pipelines.historical_pipeline import store_data_historical
from pricing_performance_holdouts.pipelines.profitable_growth_pipeline import store_data_profitable_growth


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def run_historical_data_store():
    
    project_id = "logistics-customer-staging"
    #entities = ('FP_PK', 'PY_DO')
    
    entities = tuple(set(('FP_PK','PY_DO','PY_BO', 'FP_TW', 'PY_PY', 'DJ_CZ', 'PY_EC',
    'MJM_AT' ,'PY_PE', 'PY_AR' ,'PY_GT','PY_SV' ,'FP_PH','PY_NI' ,'NP_HU' ,'FP_MM','EF_GR' ,
    'AP_PA' ,'YS_TR', 'PY_UY' ,'OP_SE' ,'PY_CL' ,'FP_BD' ,'FP_SG' ,'FO_NO' ,'PY_CR', 'FP_LA',
    'PY_HN', 'FP_MY' ,'FP_TH', 'FY_CY', 'PY_VE','PO_FI','TB_QA','TB_OM',
    'TB_KW','TB_JO','TB_IQ','TB_BH','TB_AE','HS_SA','FP_HK','FP_KH','HF_EG')))

    #vendor_v = ('Restaurant','restaurant','restaurants')
    vendor_v = ('supermarket', 'specialty','shops','shop','darkstores','Shop','Dmart')

    year = 2025

    min_date = date(2025, 5, 10)    
    max_date = date(2025, 5, 15)

    store_data_historical(
        project_id=project_id,
        entities=entities,
        year=year,
        min_date=min_date,
        max_date = max_date,
        vertical= vendor_v,
        save_local=True
    )


def run_profitable_growth_store():

    w_no = ['2025-05-12']
    vertical = 'restaurants'

    return store_data_profitable_growth(weeks = w_no,vertical_type = vertical , group = 'entity_id')

def main():
    
    #logging.info("Starting historical data storage step...")
    #run_historical_data_store()

    logging.info("Running profitable growth analysis...")
    results = run_profitable_growth_store()

    # Now you can view the output
    for week, df in results:
        print(f"\nResults for week {week}:")
        print(df.head())
    
    #logging.info("Pipeline complete. Sample results:")
    
    #print(df_result.head())


if __name__ == "__main__":
    main()