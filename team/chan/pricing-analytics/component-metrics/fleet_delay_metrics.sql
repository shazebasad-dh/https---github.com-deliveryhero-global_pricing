  with load_data as (
    SELECT 
        entity_id
      , a.scheme_id
      , scheme_price_mechanisms
      , scheme_component_ids.travel_time_config_id
      , scheme_component_ids.delay_config_id
      , scheme_component_configs.travel_time_config
      , scheme_component_configs.fleet_delay_config

    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2` a
    INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` tt_dl
      ON a.scheme_id = tt_dl.scheme_id
      AND a.entity_id = tt_dl.global_entity_id
      AND deleted = FALSE
    WHERE scheme_active_to IS NULL
    -- AND scheme_component_ids.travel_time_config_id is not null
    -- AND scheme_component_ids.mov_config_id IS NOT NULL
  )

  , load_usage_data as (
    SELECT 
     scheme_component_ids.delay_config_id
    , entity_id
    , COUNT(DISTINCT vendor_code) as n_vendors
    , COUNT(DISTINCT asa_id) as n_asa
    FROM `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` 
    LEFT JOIN UNNEST(dps_asa_configuration_history) dps
    LEFT JOIN UNNEST(asa_price_config) 
    WHERE active_to is null
    AND scheme_price_mechanisms.is_fleet_delay
    GROUP BY 1,2
  )

  ################ TRAVEL TIME METRICS
  
  , delay_data as (
    SELECT DISTINCT
        entity_id
      , a.scheme_id
      , delay_config_id
      , delay_config.delay_threshold

    FROM load_data a
    LEFT JOIN UNNEST(fleet_delay_config) ttc
    LEFT JOIN UNNEST(delay_config) delay_config
    WHERE delay_config_id is not null
    AND scheme_price_mechanisms.is_fleet_delay
  )

  , add_delay_diff as (
    SELECT *
    , delay_threshold - IFNULL(LAG(delay_threshold) OVER(component_partition), 0) as tt_diff_between_delay_tiers
    , ROW_NUMBER() OVER(component_partition) AS delay_tier
    FROM delay_data
    WINDOW component_partition AS (PARTITION BY entity_id, scheme_id, delay_config_id ORDER BY delay_threshold NULLS LAST)

  )

  , aggregate_info_per_delay_tier as (
    select 
    entity_id
    , delay_config_id
    , "Delay Time" as tier_type
    , COUNT(DISTINCT scheme_id) as n_schemes
    , MAX(delay_tier) + 1 as n_delay_tiers
    , MAX(delay_threshold)  as delay_range
    , APPROX_QUANTILES(tt_diff_between_delay_tiers, 100)[OFFSET(50)] as median_delay_tier_range
    , MIN(tt_diff_between_delay_tiers) as min_delay_tier_range
    , MAX(tt_diff_between_delay_tiers) as max_delay_tier_range
    from add_delay_diff
    GROUP BY 1,2,3
    -- order by entity_id
  )

  , tt_data as (
    SELECT 
        entity_id
      , a.scheme_id
      , delay_config_id
      , ttc.*
      , tier

    FROM load_data a
    LEFT JOIN UNNEST(fleet_delay_config) ttc
      WITH OFFSET AS tier
    WHERE delay_config_id is not null
    AND scheme_price_mechanisms.is_fleet_delay
  )

  , add_tt_diff as (
    SELECT *
      , travel_time_threshold - IFNULL(LAG(travel_time_threshold) OVER(component_partition), 0) as tt_diff_between_tiers
    FROM tt_data
    WINDOW component_partition AS (PARTITION BY entity_id, delay_config_id, scheme_id ORDER BY tier)

  )

    , aggregate_info_per_tt_tier as (
    select 
    entity_id
    , delay_config_id
    , "Travel Time" as tier_type
    , COUNT(DISTINCT scheme_id) as n_schemes
    , MAX(tier) + 1 as n_tiers
    , MAX(travel_time_threshold)  as tt_range
    , APPROX_QUANTILES(tt_diff_between_tiers, 100)[OFFSET(50)] as median_tier_range
    , MIN(tt_diff_between_tiers) as min_tier_range
    , MAX(tt_diff_between_tiers) as max_tier_range
    from add_tt_diff
    GROUP BY 1,2,3
    -- order by entity_id
  )

  , union_tier_type as (
    SELECT *
    FROM aggregate_info_per_tt_tier

    UNION ALL 
    SELECT *
    FROM aggregate_info_per_delay_tier
  )

  , tt_load_usage_data as (
    SELECT 
    scheme_component_ids.delay_config_id
    , entity_id
    , COUNT(DISTINCT vendor_code) as n_vendors
    , COUNT(DISTINCT asa_id) as n_asa
    FROM `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` 
    LEFT JOIN UNNEST(dps_asa_configuration_history) dps
    LEFT JOIN UNNEST(asa_price_config) 
    WHERE active_to is null
    GROUP BY 1,2
  )

  , tt_add_usage_data as (
    SELECT
    "Fleet Delay" as component
    , a.*
    , IFNULL(b.n_asa,0) as n_asa
    , IFNULL(b.n_vendors,0) as n_vendors
    from union_tier_type a
    LEFT JOIN tt_load_usage_data b
      ON a.delay_config_id = b.delay_config_id
      AND a.entity_id = b.entity_id
  )


  select *
  from tt_add_usage_data
  WHERE TRUE
  AND entity_id = "PY_EC"
  AND delay_config_id = 2
  ORDER BY entity_id, delay_config_id