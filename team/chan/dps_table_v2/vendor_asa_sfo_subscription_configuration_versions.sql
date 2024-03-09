----------------------------------------------------------------------------------------------------------------------------
--                NAME: vendor_asa_sfo_subscription_configuration_versions.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: David Gallo
--       CREATION DATE: 2023-05-20
--         DESCRIPTION: This table contains all historical information about Vendor ASA/SF Override/Subscriptions price configuration, on version level. 
--                      The logic is the same as vendor_asa_versions, but replicated and unioned for SF override and subscriptions.
--        QUERY OUTPUT: Every price verision at vendor level can be obtained, including SF override and subscriptions.
--               NOTES:
--             UPDATED:
--                      ----------------------------------

-- CREATE OR REPLACE TABLE `{{ params.project_id }}.cl.vendor_asa_sfo_subscription_configuration_versions`
-- CLUSTER BY entity_id, vendor_code
-- AS
-- All Config -- Union all three backend configuration tables
WITH all_vendor_assignments AS (
  SELECT * FROM `{{ params.project_id }}.cl._pricing_asa_vendor_assignments`
  UNION ALL SELECT * FROM `{{ params.project_id }}.cl._pricing_sf_override_vendor_assignments`
  UNION ALL SELECT * FROM `{{ params.project_id }}.cl._pricing_subscription_vendor_assignments`
  UNION ALL SELECT * FROM `{{ params.project_id }}.cl._pricing_priority_fee_override_vendor_assignments`
  -- Unnest Vendor -- Flatten _pricing_asa_vendor_assignments table.
), unnest_vendor AS (
  SELECT
    CASE WHEN (type IS NULL) THEN 'BASIC' ELSE type END AS type
    , entity_id
    , vendor_id AS vendor_code
    , asa_id
    , asa_name
    , active_from
    , LAG(asa_id) OVER(PARTITION BY type, entity_id, vendor_id ORDER BY active_from) AS prev_asa_id -- get the next version for each vendor, split by type.
  FROM all_vendor_assignments
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) vendor_id
  WHERE vendor_id IS NOT NULL
    AND vendor_id != ''
-- Deduplicate Vendor Configuration -- For each vendor_code, when a configuration is the same as the next consecutive one, we keep just one.
), deduplicate_vendor_config AS (
  SELECT
    *
    , IFNULL(LEAD(active_from) OVER(PARTITION BY type, entity_id, vendor_code ORDER BY active_from), '2099-01-01') AS active_to
  FROM unnest_vendor
  WHERE (
    CASE
      WHEN prev_asa_id IS NULL THEN TRUE --we keep the first version
      WHEN asa_id = prev_asa_id THEN FALSE  -- we remove version when the current one is equal to the previous one
      ELSE TRUE
    END
  )
-- ASA Price Configuration --Get all pricing ASA configurations.
), asa_price_config AS (
  SELECT
    * EXCEPT(active_to)
    , IFNULL(active_to, '2099-01-01') AS active_to
  FROM `{{ params.project_id }}.cl._pricing_asa_configuration_versions`
-- Join Price Configuration -- For each vendor and ASA, join the corresponding pricing configurations.
), join_price_config AS (
  SELECT
    vendor_config.* EXCEPT(active_from, active_to)
    , asa_price_config.* EXCEPT(active_from, active_to, asa_id, entity_id, country_code)
    , GREATEST(vendor_config.active_from, asa_price_config.active_from) AS active_from -- create new window from the ASAs that overlap. last active_from
    , LEAST(vendor_config.active_to, asa_price_config.active_to) AS active_to -- first active_to
  FROM deduplicate_vendor_config AS vendor_config
  LEFT JOIN asa_price_config ON vendor_config.asa_id = asa_price_config.asa_id
      AND vendor_config.entity_id = asa_price_config.entity_id
  WHERE TRUE
    AND vendor_config.active_from != vendor_config.active_to -- remove error configurations.
    AND vendor_config.active_from < asa_price_config.active_to -- For cases when vendor config was activated while ASA was already active.
    AND vendor_config.active_to > asa_price_config.active_from -- For cases where ASA is activated while vendor config is still active.
  -- Vendor Price Mechanisms -- For each vendor and version, flag whether it has each of the components and conditions.
), vendor_price_mechanisms AS (
  SELECT
    type
    , entity_id
    , vendor_code
    , active_from
    , MAX(asa_condition_mechanisms.asa_has_time_condition) AS vendor_has_time_condition
    , MAX(asa_condition_mechanisms.asa_has_customer_condition) AS vendor_has_customer_condition
    , MAX(asa_condition_mechanisms.asa_has_customer_area) AS vendor_has_customer_area
    , MAX(scheme_price_mechanisms.is_dbdf) AS vendor_has_dbdf
    , MAX(scheme_price_mechanisms.is_dbmov) AS vendor_has_dbmov
    , MAX(scheme_price_mechanisms.is_surge_mov) AS vendor_has_surge_mov
    , MAX(scheme_price_mechanisms.is_small_order_fee) AS vendor_has_small_order_fee
    , MAX(scheme_price_mechanisms.is_fleet_delay) AS vendor_has_fleet_delay
    , MAX(scheme_price_mechanisms.is_service_fee) AS vendor_has_service_fee
    , MAX(scheme_price_mechanisms.is_priority_fee) AS vendor_has_priority_fee
    , MAX(scheme_price_mechanisms.is_basket_value_deal) AS vendor_has_basket_value_deal
  FROM join_price_config
  LEFT JOIN UNNEST(asa_price_config) schemes
  GROUP BY 1, 2, 3, 4
-- Vendor Full ASA Config -- Aggregate all ASA configurations,
), vendor_full_asa_config AS (
  SELECT
    type
    , entity_id
    , vendor_code
    , active_from
    , IF(active_to = '2099-01-01', NULL, active_to) AS active_to
    , ARRAY_AGG(
      STRUCT(asa_id
        , asa_name
        , n_schemes
        , asa_condition_mechanisms
        , asa_price_config_hash
        , asa_price_config
      )
    ) AS dps_asa_configuration_history
  FROM join_price_config
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  type
  , entity_id
  , vendor_code
  , active_from
  , active_to
  , STRUCT(vendor_has_dbdf
    , vendor_has_dbmov
    , vendor_has_surge_mov
    , vendor_has_small_order_fee
    , vendor_has_fleet_delay
    , vendor_has_service_fee
    , vendor_has_priority_fee
    , vendor_has_basket_value_deal
    , vendor_has_time_condition
    , vendor_has_customer_condition
    , vendor_has_customer_area
  ) AS vendor_price_mechanisms
  , dps_asa_configuration_history
FROM vendor_full_asa_config
LEFT JOIN vendor_price_mechanisms USING (type, entity_id, vendor_code, active_from)