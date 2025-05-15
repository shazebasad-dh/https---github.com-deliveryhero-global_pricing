import logging
from datetime import date
import sys
from pricing_performance_holdouts.pipelines.historical_pipeline import store_data_historically

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    
    project_id = "logistics-customer-staging"
    #entities = ('FP_PK', 'PY_DO')
    
    entities = tuple(set(('FP_PK','PY_DO','PY_BO', 'FP_TW', 'PY_PY', 'DJ_CZ', 'PY_EC',
    'MJM_AT' ,'PY_PE', 'PY_AR' ,'PY_GT','PY_SV' ,'FP_PH','PY_NI' ,'NP_HU' ,'FP_MM','EF_GR' ,
    'AP_PA' ,'YS_TR', 'PY_UY' ,'OP_SE' ,'PY_CL' ,'FP_BD' ,'FP_SG' ,'FO_NO' ,'PY_CR', 'FP_LA',
    'PY_HN', 'FP_MY' ,'FP_TH', 'FY_CY', 'PY_VE','PO_FI','TB_QA','TB_OM',
    'TB_KW','TB_JO','TB_IQ','TB_BH','TB_AE','HS_SA','FP_HK','FP_KH','HF_EG')))

    year = 2025

    min_date = date(2025, 2,2)    
    #max_date = date(2025, 2, 15) 

    store_data_historically(
        project_id=project_id,
        entities=entities,
        year=year,
        min_date=min_date,
        restaurant_flag='IN',
        save_local=True
    )

if __name__ == "__main__":
    main()