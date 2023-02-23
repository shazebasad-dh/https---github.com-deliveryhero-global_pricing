----------------------------------------------------------------------------------------------------------------------------
--                NAME: XXXXX.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: XXXXX
--         DESCRIPTION: This table contains all historical information about DPS ASA vendor assignment and price configuration, on version level.
--
--        QUERY OUTPUT: Every version of every ASA configuration can be obtained.
--               NOTES: XXXXX
--                      ----------------------------------


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` 
  CLUSTER BY entity_id, asa_id
  AS
    WITH asa_vendor_config AS (
    SELECT * EXCEPT(active_to)
      , IFNULL(active_to, "2099-01-01") AS active_to
    FROM `dh-logistics-product-ops.pricing.dps_asa_vendor_assignments`
  )

  , asa_price_config AS (
    SELECT * EXCEPT(active_to)
      , IFNULL(active_to, "2099-01-01") AS active_to
    FROM `dh-logistics-product-ops.pricing.dps_asa_price_config_versions`
  )

  , join_price_config AS (
    SELECT asa_vendor_config.* EXCEPT(active_from, active_to)
    , asa_price_config.* EXCEPT(active_from, active_to, asa_id, entity_id, country_code)
    -- , asa_price_config.active_from as config_active_from
    , GREATEST(asa_vendor_config.active_from, asa_price_config.active_from) AS active_from
  
    FROM asa_vendor_config 
    LEFT JOIN asa_price_config
      ON asa_vendor_config.asa_id = asa_price_config.asa_id
      AND asa_vendor_config.entity_id = asa_price_config.entity_id
    WHERE TRUE
      AND asa_vendor_config.active_from < asa_price_config.active_to
      AND asa_vendor_config.active_to > asa_price_config.active_from  
  )

  , final_table AS (
    SELECT *
      , SHA256(
        CONCAT(
          TO_BASE64(asa_id_vendor_hash)
        , TO_BASE64(asa_price_config_hash)
        )
    ) as asa_id_config_hash
    
    FROM join_price_config
  )

  , get_past_version as (
    SELECT *
      , LAG(asa_id_config_hash) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) as prev_hash
    FROM final_table
  )

  , deduplicate_versions as (
    SELECT *
      , LEAD(active_from) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) AS active_to 
    FROM get_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE
        WHEN asa_id_config_hash = prev_hash THEN FALSE
        ELSE TRUE
      END
    )
  )

  , last_asa_assignment_version as (
    SELECT 
    entity_id
    , asa_id
    , active_to as last_active_to
    FROM asa_vendor_config
    QUALIFY ROW_NUMBER() OVER(partition by entity_id, asa_id ORDER BY active_from desc) = 1

  )

  , set_deleted_asa_timestamp as (
    SELECT dv.* EXCEPT(active_to)
    , LEAST(IFNULL(active_to,  "2099-01-01"),last_active_to) as active_to
    FROM deduplicate_versions dv
    LEFT JOIN last_asa_assignment_version lv
      ON dv.entity_id = lv.entity_id
      AND dv.asa_id = lv.asa_id

  )

  , asa_full_config as (
      SELECT entity_id
      , country_code
      , asa_id
      , active_from
      , IF(active_to = "2099-01-01", NULL, active_to) as active_to
      , asa_name
      , priority
      , vendor_group_id
      , assigned_vendors_count
      , n_schemes
      , vendor_filters
      , STRUCT(asa_id_config_hash
        , asa_id_vendor_hash
        , asa_price_config_hash
        , assigned_vendor_hash
      ) as asa_hashes
      , asa_condition_mechanisms
      , asa_price_config
      , sorted_assigned_vendor_ids

    FROM set_deleted_asa_timestamp

  )

  SELECT *
  FROM asa_full_config


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_asa_full_config_versions` 
  CLUSTER BY entity_id, asa_id
  AS
  WITH asa_vendor_config AS (
    SELECT * EXCEPT(active_to)
      , IFNULL(active_to, "2099-01-01") AS active_to
    FROM `dh-logistics-product-ops.pricing.dps_asa_vendor_assignments`
  )

  , asa_price_config AS (
    SELECT * EXCEPT(active_to)
      , IFNULL(active_to, "2099-01-01") AS active_to
    FROM `dh-logistics-product-ops.pricing.dps_asa_price_config_versions`
  )

  , join_price_config AS (
    SELECT asa_vendor_config.* EXCEPT(active_from, active_to)
    , asa_price_config.* EXCEPT(active_from, active_to, asa_id, entity_id, country_code)
    , GREATEST(asa_vendor_config.active_from, asa_price_config.active_from) AS active_from
    FROM asa_vendor_config 
    LEFT JOIN asa_price_config
      ON asa_vendor_config.asa_id = asa_price_config.asa_id
      AND asa_vendor_config.entity_id = asa_price_config.entity_id
    WHERE TRUE
      AND asa_vendor_config.active_from < asa_price_config.active_to
      AND asa_vendor_config.active_to > asa_price_config.active_from  
  )

  , final_table AS (
    SELECT *
      , SHA256(
        CONCAT(
          TO_BASE64(asa_id_vendor_hash)
        , TO_BASE64(asa_price_config_hash)
        )
    ) as asa_id_config_hash
    
    FROM join_price_config
  )

  , get_past_version as (
    SELECT *
      , LAG(asa_id_config_hash) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) as prev_hash
    FROM final_table
  )

  , deduplicate_versions as (
    SELECT *
      , LEAD(active_from) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) AS active_to 
    FROM get_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE
        WHEN asa_id_config_hash = prev_hash THEN FALSE
        ELSE TRUE
      END
    )
  )

  , asa_full_config as (
      SELECT entity_id
      , asa_id
      , active_from
      , active_to
      , asa_name
      , priority
      , vendor_group_id
      , assigned_vendors_count
      , n_schemes
      , vendor_filters
      , STRUCT(asa_id_config_hash
        , asa_id_vendor_hash
        , asa_price_config_hash
        , assigned_vendor_hash
      ) as asa_hashes
      , asa_condition_mechanisms
      , asa_price_config
      , sorted_assigned_vendor_ids

    FROM deduplicate_versions
  )

  SELECT *
  FROM asa_full_config;