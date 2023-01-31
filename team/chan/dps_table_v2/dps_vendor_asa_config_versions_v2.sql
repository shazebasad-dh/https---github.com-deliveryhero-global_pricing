----------------------------------------------------------------------------------------------------------------------------
--                NAME: XXXXX.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: XXXXX
--         DESCRIPTION: This table contains all historical information about Vendor ASA price configuration, on version level.
--
--        QUERY OUTPUT: Every price verision at vendor level can be obtained.
--               NOTES: XXXXX
--                      ----------------------------------


CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2`
CLUSTER BY entity_id, vendor_code
AS
with unnest_vendor as (
  SELECT entity_id
    , vendor_id as vendor_code
    , asa_id
    , asa_name
    , active_from
    , LAG(asa_id) OVER(PARTITION BY entity_id, vendor_id ORDER BY active_from) as prev_asa_id
  FROM `dh-logistics-product-ops.pricing.dps_asa_vendor_assignments` 
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_id
  WHERE vendor_id IS NOT NULL
  AND vendor_id <> ""

)

, deduplicate_vendor_config as (

  SELECT *
    , IFNULL(LEAD(active_from) OVER(PARTITION BY entity_id, vendor_code ORDER BY active_from), "2099-01-01") as active_to
  FROM unnest_vendor
  WHERE (
    CASE 
      WHEN prev_asa_id IS NULL THEN TRUE 
      WHEN asa_id = prev_asa_id THEN FALSE
      ELSE TRUE
    END
  ) 
)

,  asa_price_config as ( 
  SELECT * EXCEPT(active_to)
    , IFNULL(active_to, "2099-01-01") AS active_to
  FROM `dh-logistics-product-ops.pricing.dps_asa_price_config_versions`
)

, join_price_config AS (
  SELECT vendor_config.* EXCEPT(active_from, active_to)
  , asa_price_config.* EXCEPT(active_from, active_to, asa_id, entity_id, country_code)
  , GREATEST(vendor_config.active_from, asa_price_config.active_from) AS active_from
  , LEAST(vendor_config.active_to, asa_price_config.active_to) AS active_to
  FROM deduplicate_vendor_config vendor_config
  LEFT JOIN asa_price_config
    ON vendor_config.asa_id = asa_price_config.asa_id
    AND vendor_config.entity_id = asa_price_config.entity_id
  WHERE TRUE 
    AND vendor_config.active_from <> vendor_config.active_to
    AND vendor_config.active_from < asa_price_config.active_to
    AND vendor_config.active_to > asa_price_config.active_from  
)

, vendor_price_mechanisms as (
    SELECT entity_id
      , vendor_code
      , active_from
      , MAX(asa_condition_mechanisms.asa_has_time_condition) as vendor_has_time_condition
      , MAX(asa_condition_mechanisms.asa_has_customer_condition) as vendor_has_customer_condition
      , MAX(asa_condition_mechanisms.asa_has_customer_area) as vendor_has_customer_area
      , MAX(scheme_price_mechanisms.is_dbdf) as vendor_has_dbdf
      , MAX(scheme_price_mechanisms.is_dbmov) as vendor_has_dbmov
      , MAX(scheme_price_mechanisms.is_surge_mov) as vendor_has_surge_mov
      , MAX(scheme_price_mechanisms.is_small_order_fee) as vendor_has_small_order_fee
      , MAX(scheme_price_mechanisms.is_fleet_delay) as vendor_has_fleet_delay
      , MAX(scheme_price_mechanisms.is_service_fee) as vendor_has_service_fee
      , MAX(scheme_price_mechanisms.is_basket_value_deal) as vendor_has_basket_value_deal
  FROM join_price_config
  LEFT JOIN UNNEST(asa_price_config) schemes
  GROUP BY 1,2,3
)

, vendor_full_asa_config as (
    SELECT entity_id
    , vendor_code
    , active_from
    , IF(active_to = "2099-01-01", NULL, active_to) as active_to 
    , ARRAY_AGG(
        STRUCT(asa_id
          , asa_name
          , n_schemes
          , asa_condition_mechanisms
          , asa_price_config_hash
          , asa_price_config
      )
    ) as dps_asa_configuration_history
FROM join_price_config
GROUP BY 1, 2, 3, 4
)

, add_vendor_price_mechanisms as (
  SELECT
  entity_id
    , vendor_code
    , active_from
    , active_to
    , STRUCT( vendor_has_dbdf
      , vendor_has_dbmov
      , vendor_has_surge_mov
      , vendor_has_small_order_fee
      , vendor_has_fleet_delay
      , vendor_has_service_fee
      , vendor_has_basket_value_deal
      , vendor_has_time_condition
      , vendor_has_customer_condition
      , vendor_has_customer_area
    ) as vendor_price_mechanisms
    , dps_asa_configuration_history
    FROM vendor_full_asa_config
    LEFT JOIN vendor_price_mechanisms
      USING(entity_id, vendor_code, active_from)
)



SELECT *
FROM add_vendor_price_mechanisms
