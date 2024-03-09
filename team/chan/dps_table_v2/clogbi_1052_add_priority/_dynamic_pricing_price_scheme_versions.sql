----------------------------------------------------------------------------------------------------------------------------
--                NAME: _dynamic_pricing_price_scheme_versions.sql
--      INITIAL AUTHOR: Trupti Karnire
--       CREATION DATE: 2021-03-26
--         DESCRIPTION: Versioning table for all DPS Price Schemes
--        QUERY OUTPUT: All price schemes with their corresponding component ids and the periods when they were active.
--               NOTES:
--             UPDATED: 2021-03-26 | Trupti Karnire     | BILOG-1051  | Create DPS Version Price Scheme Table
--                      2022-06-02 | Fatima Rodriguez   | CLOGBI-615  | Fix active_to in DPS Config Versions
--                      2022-06-03 | Fatima Rodriguez   | CLOGBI-618  | Fix deleted_at for Scheme configs
--                      2024-02-21 | Sebastian Lafaurie | CLOGBI-1052 | Add priority fee
--                      ----------------------------------

-- CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._dynamic_pricing_price_scheme_versions`
CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing._dynamic_pricing_price_scheme_versions`
CLUSTER BY country_code, region, entity_id AS
WITH price_scheme_data AS (
  SELECT
    country_code
    , region
    , scheme_id
    , name
    , global_entity_id AS entity_id
    , delay_fee_configuration_id
    , mov_configuration_id
    , travel_time_fee_configuration_id
    , basket_value_fee_configuration_id
    , service_fee_configuration_id
    , priority_fee_configuration_id
    , created_at
    , updated_at
    , merge_layer_run_from
    , deleted
  -- FROM `{{ params.project_id }}.hl.dynamic_pricing_price_scheme`
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_price_scheme`

), is_deleted_records AS (
  SELECT
    country_code
    , region
    , entity_id
    , scheme_id
    , name AS scheme_name
    , travel_time_fee_configuration_id
    , mov_configuration_id
    , delay_fee_configuration_id
    , basket_value_fee_configuration_id
    , service_fee_configuration_id
    , priority_fee_configuration_id
    , created_at
    , updated_at
    , IF(deleted, updated_at, NULL) AS deleted_at
    , deleted AS is_deleted
    , ROW_NUMBER() OVER (PARTITION BY country_code, entity_id, scheme_id ORDER BY updated_at) AS _row_number
  FROM price_scheme_data
), versioning_data AS (
  SELECT
    country_code
    , region
    , entity_id
    , scheme_id
    , scheme_name
    , travel_time_fee_configuration_id
    , mov_configuration_id
    , delay_fee_configuration_id
    , basket_value_fee_configuration_id
    , service_fee_configuration_id
    , priority_fee_configuration_id
    -- If the record is the first insert, then the active_from starts from when the record was created.
    , IF(_row_number = 1, created_at, updated_at) AS active_from
    , LEAD(updated_at) OVER (PARTITION BY entity_id, scheme_id ORDER BY updated_at) AS active_to
    , is_deleted
    , deleted_at
  FROM is_deleted_records
), setting_active_to AS (
  SELECT
    country_code
    , region
    , entity_id
    , scheme_id
    , scheme_name
    , travel_time_fee_configuration_id
    , mov_configuration_id
    , delay_fee_configuration_id
    , basket_value_fee_configuration_id
    , service_fee_configuration_id
    , priority_fee_configuration_id
    , deleted_at
    , active_from
    -- if the record has been permanently deleted, the active_to will be set to deleted_at(the last time the record was
    -- last seen in the DB)
    , IF(active_to IS NULL AND is_deleted, deleted_at, active_to) AS active_to
    , is_deleted
  FROM versioning_data
)
SELECT
  country_code
  , region
  , entity_id
  , scheme_id
  , ARRAY_AGG(
    STRUCT(scheme_name
      , travel_time_fee_configuration_id
      , mov_configuration_id
      , delay_fee_configuration_id
      , basket_value_fee_configuration_id
      , service_fee_configuration_id
      , priority_fee_configuration_id
      , active_from
      , active_to
    ) ORDER BY active_from
  ) AS price_scheme_history
FROM setting_active_to
GROUP BY 1, 2, 3, 4