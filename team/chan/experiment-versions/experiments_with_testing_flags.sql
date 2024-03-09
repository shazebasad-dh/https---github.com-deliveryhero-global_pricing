WITH load_asa_mechanisms AS (
    --  Load current Data
    -- ASA component mechanisms
    SELECT
      entity_id
      , vendor_code as vendor_id
      , active_from
      , asa_id
      , IFNULL(active_to, CURRENT_TIMESTAMP()) AS active_to
      , CAST(customer_condition_id AS STRING) customer_condition_id
      , CAST(schedule_id AS STRING) schedule_id
      , IF(ARRAY_LENGTH(area_configs) > 0
          , ARRAY_TO_STRING(ARRAY(SELECT CAST(x.area_id AS STRING) FROM UNNEST(area_configs) x ORDER BY x.area_id),", ")
          , NULL
      ) AS customer_area_ids

      -- , ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS) 
      -- , ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS) 
      -- , ARRAY_CONCAT_AGG(ARRAY(SELECT x.area_id FROM UNNEST(area_configs) x) IGNORE NULLS) 

    FROM `fulfillment-dwh-production.cl.vendor_asa_sfo_subscription_configuration_versions`
    LEFT JOIN UNNEST(dps_asa_configuration_history) asa
    LEFT JOIN UNNEST(asa.asa_price_config) apc
    -- FROM `{{ params.project_id }}.cl.vendor_asa_sfo_subscription_configuration_versions`
    WHERE type = 'BASIC'
)

, asa_condition_hashes AS (

  SELECT 
  entity_id
  , vendor_id
  , active_from
  , active_to
  , asa_id
  , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") asa_customer_condition_ids
  , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") asa_time_condition_ids
  , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") asa_customer_area_ids
  , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") ) asa_customer_condition_hash
  , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") ) asa_time_condition_hash
  , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") ) asa_customer_area_hash
  FROM load_asa_mechanisms
  GROUP BY 1,2,3,4,5
)

, load_vendor_experiment_price_mechanisms AS (
  SELECT *
  FROM `logistics-data-storage-staging.temp_pricing.vendor_experiment_configuration_versions`
)

, join_asa_mechanisms AS (
  SELECT exper.*
  , asa.* EXCEPT(entity_id, vendor_id)
  FROM load_vendor_experiment_price_mechanisms exper
  LEFT JOIN asa_condition_hashes asa
    ON exper.entity_id = asa.entity_id 
    AND exper.vendor_id = asa.vendor_id 
  WHERE exper.test_sub_period_from < asa.active_to 
    AND exper.test_sub_period_to > asa.active_from
)

, add_condition_flags AS (
  SELECT *
  , CASE
      WHEN test_time_condition_hash IS NULL AND asa_time_condition_hash IS NULL THEN FALSE 
      WHEN test_time_condition_hash IS NULL OR asa_time_condition_hash IS NULL THEN TRUE 
      ELSE test_time_condition_hash <> asa_time_condition_hash
    END AS vendor_has_different_time_condition_than_asa

  , CASE
      WHEN test_customer_condition_hash IS NULL AND asa_customer_condition_hash IS NULL THEN FALSE 
      WHEN test_customer_condition_hash IS NULL OR asa_customer_condition_hash IS NULL THEN TRUE 
      ELSE test_customer_condition_hash <> asa_customer_condition_hash
    END AS vendor_has_different_customer_condition_than_asa

  , CASE
      WHEN test_customer_area_hash IS NULL AND asa_customer_area_hash IS NULL THEN FALSE 
      WHEN test_customer_area_hash IS NULL OR asa_customer_area_hash IS NULL THEN TRUE 
      ELSE test_customer_area_hash <> asa_customer_area_hash
    END AS vendor_has_different_customer_area_than_asa

  FROM join_asa_mechanisms
)

, final_table AS (
SELECT 
entity_id 
, test_id
, test_name
, vendor_id
, GREATEST(test_sub_period_from, active_from) test_sub_period_from
, LEAST(test_sub_period_to, active_to) test_sub_period_to
, is_dbdf
, is_dbmov
, is_surge_mov
, is_small_order_fee
, is_fleet_delay
, is_basket_value
, is_priority_fee
, vendor_has_test_time_condition
, vendor_has_test_customer_condition
, vendor_has_test_customer_area
, travel_time_differs_across_variants
, dbmov_differs_across_variants
, surge_mov_differs_across_variants
, small_order_fee_differs_across_variants
, fleet_delay_differs_across_variants
, basket_value_differs_across_variants
, priority_fee_differs_across_variants
, vendor_has_different_time_condition_than_asa
, vendor_has_different_customer_condition_than_asa
, vendor_has_different_customer_area_than_asa
, test_time_condition_hash
, asa_time_condition_hash
, asa_time_condition_ids
, test_time_condition_ids

, test_customer_condition_hash
, asa_customer_condition_hash
, asa_customer_condition_ids
, test_customer_condition_ids

, test_customer_area_hash
, asa_customer_area_hash
, test_customer_area_ids
, asa_customer_area_ids
, asa_id

FROM add_condition_flags
)

, aggregate_table AS (

SELECT 
entity_id 
, test_id
, test_name 
, test_sub_period_from 
, test_sub_period_to
, SHA256(STRING_AGG(vendor_id,",")) as vendor_cohort_hash --vendor who essentially have all the same version window
, COUNT(vendor_id) vendor_count
, COUNT(IF(vendor_has_different_time_condition_than_asa, vendor_id, NULL)) as vendor_with_time_condition_change_count
, COUNT(IF(vendor_has_different_customer_condition_than_asa, vendor_id, NULL)) as vendor_with_customer_condition_count
, COUNT(IF(vendor_has_different_customer_area_than_asa, vendor_id, NULL)) as vendor_with_customer_area_change_count
FROM final_table
GROUP BY 1,2,3,4,5
)

SELECT *
FROM aggregate_table
WHERE TRUE
-- AND test_name = "SA_20230712_R_00_O_Jeddah_ Locals elasticity test"
AND test_name = "SA_20231003_Z_GZ_O_Jeddah(R:Locals,LS:BV DF)"
-- AND vendor_id = "655295"
-- AND vendor_id = "92614"
-- ORDER BY vendor_id, test_sub_period_from
ORDER BY vendor_cohort_hash, test_sub_period_from
-- ORDER BY active_from
LIMIT 100