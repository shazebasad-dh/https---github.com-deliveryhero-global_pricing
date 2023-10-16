

####################################### _sl_dps_ab_test_significance_orders_results


  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing._sl_dps_ab_test_significance_orders_results`
  -- AS
  -- SELECT * FROM `dh-logistics-product-ops.pricing._sl_dps_ab_test_significance_orders_results`;

#######################################

####################################### dps_weekly_fees

  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.dps_weekly_fees`
  -- PARTITION BY week
  -- CLUSTER BY region, management_entity
  -- OPTIONS(
  --   expiration_timestamp = NULL
  -- )
  -- AS
  -- SELECT * FROM `dh-logistics-product-ops.pricing.dps_weekly_fees`;

#######################################

####################################### qdd_pre_staging_table

  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.qdd_pre_staging_table`
  -- PARTITION BY created_date_local
  -- CLUSTER BY entity_id, test_name
  -- AS
  -- SELECT * FROM `dh-logistics-product-ops.pricing.qdd_pre_staging_table`;

#######################################

####################################### talabat_qdd_orders

  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.talabat_qdd_orders`
  -- PARTITION BY created_date_local
  -- CLUSTER BY entity_id, test_name
  -- AS
  -- SELECT * FROM `dh-logistics-product-ops.pricing.talabat_qdd_orders`;

#######################################

####################################### _tr_vale_vendor_commission_list

    -- CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing._tr_vale_vendor_commission_list`
    --     (
    --     vendor_id STRING,
    --     commission_rate FLOAT64
    --     )
    --     OPTIONS (
    --     format="GOOGLE_SHEETS",
    --     uris=["https://docs.google.com/spreadsheets/d/12BPyxw-Tfe7ndiHXW87oHsr86ZjVmPbaKbEUBu7LoOI/edit#gid=1206800966"],
    --     sheet_range="Sheet2!A1:B22408",
    --     skip_leading_rows=1
    -- );
#######################################

####################################### hs_sa_rdf_orders


  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders`
  -- AS
  -- SELECT * FROM `dh-logistics-product-ops.pricing.hs_sa_rdf_orders`;

#######################################

####################################### _sa_qdd_pre_staging_table

    -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing._sa_qdd_pre_staging_table`
    -- PARTITION BY created_date_local
    -- CLUSTER BY test_name
    -- AS
    -- SELECT * FROM `dh-logistics-product-ops.pricing._sa_qdd_pre_staging_table`;

#######################################

####################################### sl_pricing_mechanism_data_prd

    -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd`
    -- PARTITION BY created_date_local
    -- CLUSTER BY entity_id, platform_order_code
    -- OPTIONS (
    --   description = "This table keeps the information related to DPS price mechanisms usage of an order at an order level."
    --   )
    -- AS
    -- SELECT * FROM `dh-logistics-product-ops.pricing.sl_pricing_mechanism_data_prd`;

####################################### 
