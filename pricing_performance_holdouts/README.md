# Pricing Performance Holdouts Pipeline

This repository contains the full data extraction, cleaning, CUPED adjustment, and storage pipeline for pricing holdout experiments across food delivery platforms.

The pipeline supports Google BigQuery data extraction, pandas data processing, bootstrapping, Welch’s t-tests, and local or GCS parquet storage.

---

## 📂 Project Structure

pricing_performance_holdouts/
├── data/
│ ├── init.py
│ ├── bigquery_queries.py # SQL query generators
│ ├── extract.py # BigQuery client + data extraction
│ ├── transform.py # data cleaning + filtering
│ ├── cuped.py # CUPED variance reduction methods
│ └── store.py # upload to GCS or save locally
│
├── analysis/
│ ├── init.py
│ ├── bootstrap.py # bootstrap confidence intervals
│ ├── ttest.py # Welch's t-test + plotting
│ └── plotting.py # plotting utilities
│
├── utils/
│ ├── init.py
│ ├── dates.py # ISO week utilities
│ └── logging_config.py # centralized logging setup
│
├── tests/ # unit tests
│
├── outputs/ # local parquet files
│
├── pipelines/
│ ├── init.py
│ ├── historical_pipeline.py # Weekly cummulative historical data extraction pipeline
│ └── run_pipeline.py # CLI entrypoint to run pipeline
│
├── config.yaml # config file (project_id, bucket, paths etc.)
├── requirements.txt
└── README.md

## 💻 Setup

1️⃣ Clone the repository:
```bash
git clone https://github.com/your-org/pricing_performance_holdouts.git
cd pricing_performance_holdouts

2️⃣ Create a virtual environment:
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

3️⃣ Install dependencies:
pip install -r requirements.txt

⚙️ Configuration
Edit config.yaml to set:
    your GCP project
    BigQuery dataset/table names
    local output paths
    GCS bucket names (optional)

Example config.yaml:
project_id: "your-gcp-project-id"
gcs_bucket: "holdout_data"
local_output_dir: "./outputs"

You can run the full backfill pipeline directly from command line:
cd pricing_performance_holdouts
python -m pipelines.run_pipeline

Alternatively inside a Python script or notebook:

from pipelines.historical_pipeline import store_data_historically

store_data_historically(
    project_id="your-gcp-project-id",
    entities=["entity_1", "entity_2"],
    year=2025,
    restaurant_flag="IN",
    save_local=True
)

👥 Contributors
Your Shazeb Asad (@shazebasad-dh)



