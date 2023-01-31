----------------------------------------------------------------------------------------------------------------------------
--                NAME: XXXXXXXX.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: XXXXXXXX
--         DESCRIPTION: This table contains all historical information about ASA price scheme configurations, on version level.
--
--        QUERY OUTPUT: Every version of every pricing configuration within an ASA can be obtained.
--               NOTES: ASAs can contain multiple schemes. 
--                      ----------------------------------


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_asa_price_config_versions` 
  CLUSTER BY country_code, entity_id, asa_id
  AS
  WITH load_asa AS (
    SELECT country_code
      , region
      , global_entity_id AS entity_id
      , vendor_group_price_config_id AS asa_price_config_id
      , vendor_group_assignment_id AS asa_id
      , priority
      , variant
      , price_scheme_id AS scheme_id
      , customer_condition_id
      , schedule_id
      , TIMESTAMP_TRUNC(updated_at, MINUTE) as active_from
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group_price_config`
    WHERE deleted = FALSE
    AND ((variant IS NULL) OR (variant = "Original"))

  )

  , set_active_to AS (
    -- granularity is at each scheme part of the asa but we want to have the versioning of a given asa price config
    SELECT *
    , IFNULL(LEAD(active_from) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from), "2099-01-01") as active_to
    FROM (
      SELECT DISTINCT
        entity_id
        , asa_id
        , active_from
      FROM load_asa
    )
  )

  , add_active_to_asa AS (
    SELECT *
    FROM load_asa
    LEFT JOIN set_active_to
      USING(entity_id, asa_id, active_from)
  )

  ------- GENERATE NEW TIMESTAMP
  , load_scheme_config AS (
    SELECT entity_id
      , scheme_id
      , scheme_name
      , scheme_active_from AS scheme_active_from
      , IFNULL(scheme_active_to, "2099-01-01") AS scheme_active_to
      , scheme_config_hash
      , scheme_price_mechanisms
      , scheme_component_ids
      , scheme_component_hashes
      , scheme_component_configs
    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2`
    -- We omit versions with less than 1 minute lifetime
    WHERE (
      (TIMESTAMP_DIFF(scheme_active_to, scheme_active_from, MINUTE) > 1)
      OR scheme_active_to IS NULL
    )
  )

  , add_scheme_config AS (
    SELECT asa.* EXCEPT(active_from, active_to)
      , scheme.* EXCEPT(scheme_active_from, scheme_active_to, scheme_id, entity_id)
      , active_from AS initial_asa_versioning -- maintain the original asa timestamp version
      , GREATEST(scheme_active_from, active_from) AS active_from -- create new window from the schemes that overlap
      , LEAST(scheme_active_to, active_to) AS active_to
    FROM add_active_to_asa asa
    LEFT JOIN load_scheme_config scheme
      ON asa.entity_id = scheme.entity_id 
      AND asa.scheme_id = scheme.scheme_id 
    WHERE TRUE -- leave only overlapping schemes and asa
    AND scheme_active_from < active_to 
    AND scheme_active_to > active_from
  )

  , agg_by_asa_version AS (
    -- get a list off all the timestamp that are present within each asa original timestamp
    -- the original asa timestamp only takes into account changes of the ASA price config 
    SELECT entity_id
    , asa_id 
    , initial_asa_versioning
    , ARRAY_AGG(active_from) active_from_agg
    , ARRAY_AGG(active_to) active_to_agg
    FROM add_scheme_config
    GROUP BY 1, 2, 3
  )

  , get_ordered_timestamp AS (
    -- get a sorted timestamp of all the timestamp
    -- now each timestamp represent a single of the schemes and asa configuration
    SELECT entity_id
      , asa_id
      , initial_asa_versioning
      , ARRAY(
        SELECT DISTINCT 
        DATETIME_TRUNC(x, MINUTE) as x 
        FROM UNNEST(ARRAY_CONCAT(active_from_agg, active_to_agg)) x ORDER BY x
      ) AS ordered_timestamp 
    FROM agg_by_asa_version

  )

  , add_new_active_to AS (
    SELECT * EXCEPT(ordered_timestamp)
      , LEAD(new_active_from, 1) OVER(PARTITION BY entity_id, asa_id, initial_asa_versioning ORDER BY new_active_from) AS new_active_to
    FROM get_ordered_timestamp
    LEFT JOIN UNNEST(ordered_timestamp) new_active_from

  )

  , asa_new_versioning AS (
    SELECT *
    FROM add_new_active_to
    WHERE TRUE
      -- remove versioning over 2099-01-01 which is already the last
    AND new_active_to IS NOT NULL 
    AND new_active_from < new_active_to
  ) 

  --------- ADD ORIGINAL ASA CONFIG
  , add_asa_config_at_the_time AS (
    SELECT region
      , country_code
      , entity_id
      , asa_id 
      , new_active_from 
      , new_active_to 
      , asa_price_config_id 
      , priority
      , scheme_id 
      , schedule_id
      , customer_condition_id
    FROM asa_new_versioning
    LEFT JOIN add_active_to_asa
      USING(entity_id, asa_id)
    WHERE TRUE
    -- -- Bring THE asa version within the new windows
    AND active_from < new_active_to 
    AND active_to > new_active_from
  )

  ---- ADD schedule_config 

  , get_schedule_config as (
    SELECT country_code
    , schedule_id
    , STRUCT(start_at
      , end_at 
      , timezone
      , recurrence
      , active_days
      , is_all_day
      , recurrence_end_at
    ) schedule_config
  FROM `fulfillment-dwh-production.cl._dynamic_pricing_schedule_versions` 
  LEFT JOIN UNNEST(schedule_config_history)
  WHERE active_to IS NULL 
  )

  , add_schedule_config as (
    SELECT asa.*
    , sch.schedule_config
    FROM add_asa_config_at_the_time asa
    LEFT JOIN get_schedule_config sch 
      ON asa.country_code = sch.country_code
      AND asa.schedule_id = sch.schedule_id
  )

  ---- ADD customer_condition config

  ,get_customer_condition_config as (     
    SELECT country_code
    , customer_condition_id
    , STRUCT(description
      , orders_number_less_than 
      , days_since_first_order_less_than
    ) customer_condition_config
  FROM `fulfillment-dwh-production.cl._dynamic_pricing_customer_condition_versions`
  LEFT JOIN UNNEST(customer_condition_config_history)
  WHERE active_to IS NULL
  )

  , add_customer_condition_config as (
    SELECT asa.*
    , ccc.customer_condition_config
    FROM add_schedule_config asa
    LEFT JOIN get_customer_condition_config ccc 
      ON asa.country_code = ccc.country_code
      AND asa.customer_condition_id = ccc.customer_condition_id
  )

  ---- ADD area config

  , get_area_shapes as (
    SELECT country_code
    , area_id
    , city_id 
    , name
    , ST_GEOGFROM(polygon) as polygon
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_customer_area` 
    WHERE deleted = FALSE
  )

  , get_customer_area as (
    SELECT area.country_code
    , vendor_group_price_config_id
    , ARRAY_AGG(
      STRUCT(
      area.area_id
      , name
      , city_id
      , polygon
      )
      ORDER BY area.area_id
    ) as area_configs

    FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group_price_config_customer_area`  area
    LEFT JOIN get_area_shapes  gts
      ON area.country_code = gts.country_code
      AND area.area_id = gts.area_id
    WHERE deleted = FALSE
    GROUP BY 1,2
  )

  , add_customer_area as (
    SELECT asa.*
    , gca.area_configs
    FROM add_customer_condition_config asa
    LEFT JOIN get_customer_area gca 
      ON asa.country_code = gca.country_code
      AND asa.asa_price_config_id = gca.vendor_group_price_config_id
  )

  ---- ADD scheme config

  , add_scheme_config_at_the_time AS (
    SELECT asa.* 
      , scheme.* EXCEPT(scheme_active_from, scheme_active_to, scheme_id, entity_id)
    FROM add_customer_area asa
    LEFT JOIN load_scheme_config scheme
      ON asa.entity_id = scheme.entity_id 
      AND asa.scheme_id = scheme.scheme_id 
    WHERE TRUE
    AND scheme_active_from < new_active_to 
    AND scheme_active_to > new_active_from
    --- Bring THE scheme version within the new windows
    -- AND scheme_active_from BETWEEN new_active_from and new_active_to
  )

  ---- CLEAN SCHEMES

  , clean_schema AS (
  SELECT region
    , country_code
    , entity_id
    , asa_id
    , new_active_from  AS active_from
    , IF(new_active_to = "2099-01-01", NULL, new_active_to) AS active_to
    , COUNT(DISTINCT scheme_id) AS n_schemes
    , COUNT(DISTINCT CASE WHEN schedule_id IS NOT NULL THEN scheme_id END) AS n_schemes_with_time_condition
    , COUNT(DISTINCT CASE WHEN customer_condition_id IS NOT NULL THEN scheme_id END ) AS n_schemes_with_customer_condition
    , COUNT(DISTINCT CASE WHEN area_configs IS NOT NULL THEN scheme_id END ) AS n_schemes_with_customer_area
    , ARRAY_AGG(
        CONCAT(asa_price_config_id, TO_BASE64(scheme_config_hash))
        ORDER BY asa_price_config_id
      ) as _asa_price_config_hash
    , ARRAY_AGG(
      STRUCT(
      asa_price_config_id
      , scheme_id
      , scheme_config_hash
      , priority
      , CASE 
        WHEN customer_condition_id IS NULL 
            AND schedule_id IS NULL 
            AND area_configs IS NULL
          THEN TRUE 
          ELSE FALSE 
        END AS is_default_scheme
      , customer_condition_id
      , customer_condition_config
      , schedule_id
      , schedule_config
      , ARRAY_LENGTH(area_configs) as n_areas
      , area_configs
      , scheme_price_mechanisms
      , scheme_component_ids
      , scheme_component_hashes
      , scheme_component_configs
      ) 
      ORDER BY priority
    )
    AS asa_price_config
  FROM add_scheme_config_at_the_time
  GROUP BY 1, 2, 3, 4, 5, 6
  )

  ---- HASHES AND DEDUPLICATION

  , add_hash as (
    SELECT *
      , SHA256(
        ARRAY_TO_STRING(_asa_price_config_hash,"")
      ) as asa_price_config_hash
    FROM clean_schema
  )

  , get_next_and_past_version AS (
    SELECT *
      , LAG(asa_price_config_hash) OVER(asa_price_partition) AS prev_hash
    FROM add_hash
    WINDOW asa_price_partition AS(PARTITION BY entity_id, asa_id ORDER BY active_from)
  )

  , deduplicate_versions AS (
    SELECT *
    FROM get_next_and_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN asa_price_config_hash = prev_hash then FALSE -- remove duplicates
        ELSE TRUE
        END
    )
  )

  , asa_final_table as (
    SELECT region
      , country_code
      , entity_id
      , asa_id
      , active_from
      , LEAD(active_from) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) AS active_to
      , n_schemes
      , STRUCT(
        IF(n_schemes_with_time_condition > 0, TRUE, FALSE) as asa_has_time_condition
        , IF(n_schemes_with_customer_condition > 0, TRUE, FALSE) as asa_has_customer_condition
        , IF(n_schemes_with_customer_area > 0, TRUE, FALSE) as asa_has_customer_area
      ) as asa_condition_mechanisms
      , asa_price_config_hash
      , asa_price_config
  FROM deduplicate_versions
  )

  ---- final 
  SELECT *
  FROM asa_final_table
  ;