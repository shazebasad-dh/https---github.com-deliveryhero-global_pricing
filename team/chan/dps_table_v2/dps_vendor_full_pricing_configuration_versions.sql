
CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.vendor_full_pricing_configuration_versions`
CLUSTER BY entity_id, vendor_code
AS
with service_fee_overrides_versions as (
  SELECT * EXCEPT(active_to)
  , IFNULL(active_to, "2099-01-01") as active_to
  , TRUE as has_service_fee_override -- dummy variable ease logic
  FROM `dh-logistics-product-ops.pricing.dps_vendor_service_fee_overrides_configuration_versions`
)

, asa_versions as (
  select * EXCEPT(active_to)
  , IFNULL(active_to, "2099-01-01") as active_to
  from  `fulfillment-dwh-production.cl.vendor_asa_configuration_versions`
)

, join_price_config AS (
  SELECT asa_versions.* EXCEPT(active_from, active_to)
    , sf_versions.* EXCEPT(vendor_code, active_from, active_to, entity_id)
    , CASE 
        WHEN has_service_fee_override IS NULL 
        THEN asa_versions.active_from
        ELSE GREATEST(asa_versions.active_from, sf_versions.active_from) 
    END AS active_from
    , CASE
      WHEN has_service_fee_override IS NULL 
      THEN asa_versions.active_to
      ELSE LEAST(asa_versions.active_to, sf_versions.active_to) 
    END AS active_to
  FROM asa_versions
  LEFT JOIN service_fee_overrides_versions sf_versions
    ON asa_versions.vendor_code = sf_versions.vendor_code
    AND asa_versions.entity_id = sf_versions.entity_id
  WHERE (
    (has_service_fee_override IS NULL)
    OR (
      (asa_versions.active_from < sf_versions.active_to)
      AND (asa_versions.active_to > sf_versions.active_from) 
    )
  )
)


SELECT 
  entity_id
  , vendor_code
  , active_from 
  , IF(active_to = "2099-01-01", NULL, active_to) as active_to 
  , IFNULL(has_service_fee_override, FALSE) as has_service_fee_override
  , STRUCT(
      vendor_price_mechanisms.vendor_has_time_condition
    , vendor_price_mechanisms.vendor_has_customer_condition
    , vendor_price_mechanisms.vendor_has_customer_area
    , vendor_price_mechanisms.vendor_has_dbdf
    , vendor_price_mechanisms.vendor_has_dbmov
    , vendor_price_mechanisms.vendor_has_surge_mov
    , vendor_price_mechanisms.vendor_has_small_order_fee
    , vendor_price_mechanisms.vendor_has_fleet_delay
    , IFNULL(has_service_fee_override, vendor_price_mechanisms.vendor_has_service_fee) as vendor_has_service_fee
    , vendor_price_mechanisms.vendor_has_basket_value_deal
  ) as vendor_price_mechanisms
  , dps_asa_configuration_history
  , dps_service_fee_overrides_history
FROM join_price_config;