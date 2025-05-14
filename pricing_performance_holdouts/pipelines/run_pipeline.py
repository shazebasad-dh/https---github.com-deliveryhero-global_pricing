import logging
from datetime import date
import sys
from pricing_performance_holdouts.pipelines.historical_pipeline import store_data_historically

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    
    project_id = "logistics-customer-staging"
    entities = ('FP_PK', 'PY_DO')
    
    year = 2025

    min_date = date(2025, 2,2)    
    max_date = date(2025, 2, 3) 

    store_data_historically(
        project_id=project_id,
        entities=entities,
        year=year,
        min_date=min_date,
        max_date=max_date,
        restaurant_flag='IN',
        save_local=True
    )

if __name__ == "__main__":
    main()