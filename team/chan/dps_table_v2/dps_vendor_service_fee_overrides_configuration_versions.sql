CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_vendor_service_fee_overrides_configuration_versions`
CLUSTER BY entity_id, vendor_code
AS
with unnest_vendor as (
  SELECT entity_id
    , vendor_id as vendor_code
    , asa_id
    , asa_name
    , active_from
    , LAG(asa_id) OVER(PARTITION BY entity_id, vendor_id ORDER BY active_from) as prev_asa_id
  FROM `dh-logistics-product-ops.pricing.dps_service_fee_overrides_vendor_assignments`
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
  FROM `fulfillment-dwh-production.cl._pricing_asa_configuration_versions`
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

, get_service_fee_assignment_config as (
    SELECT entity_id
      , vendor_code
      , active_from
      , asa_id
      , ARRAY_AGG(
        STRUCT(asa_price_config_id
        , scheme_id
        , scheme_config_hash
        , priority
        , is_default_scheme
        , customer_condition_id
        , customer_condition_config
        , schedule_id
        , schedule_config
        , n_areas
        , area_configs
        , scheme_price_mechanisms.service_fee_type
        , scheme_component_ids.service_fee_config_id
        , scheme_component_hashes.service_fee_config_hash
        , scheme_component_configs.service_fee_config
        )
      ) as service_fee_assignment_config
  FROM join_price_config
  LEFT JOIN UNNEST(asa_price_config) schemes
  GROUP BY 1,2,3,4
)

, vendor_full_asa_config as (
    SELECT entity_id
    , vendor_code
    , active_from
    , IF(active_to = "2099-01-01", NULL, active_to) as active_to 
    , ARRAY_AGG(
        STRUCT(asa_id as assignment_id
          , asa_name as assignment_name
          , n_schemes
          , asa_price_config_hash as assignment_price_config_hash
          , service_fee_assignment_config
      )
    ) as dps_service_fee_overrides_history
FROM join_price_config
LEFT JOIN get_service_fee_assignment_config
  USING(entity_id, vendor_code, active_from, asa_id)
GROUP BY 1, 2, 3, 4
)

SELECT *
FROM vendor_full_asa_config