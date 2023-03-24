#################################################### TRAVEL TIME METRICS ####################################################
  CREATE OR REPLACE TEMP TABLE travel_time_metrics AS 
  with tt_load_data as (
    SELECT 
        entity_id
      , a.scheme_id
      , scheme_component_ids.travel_time_config_id
      , ttc.*
      , tier

    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2` a
    LEFT JOIN UNNEST(scheme_component_configs.travel_time_config) ttc
      WITH OFFSET as tier
    INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` tt_dl
      ON a.scheme_id = tt_dl.scheme_id
      AND a.entity_id = tt_dl.global_entity_id
      AND deleted = FALSE
    WHERE scheme_active_to IS NULL
    AND scheme_component_ids.travel_time_config_id is not null
  )

  , tt_add_diff_between_tier as (
    SELECT *
    , travel_time_threshold - IFNULL(LAG(travel_time_threshold) OVER(component_partition), 0) as tt_diff_between_tiers
    , IFNULL(LAG(travel_time_threshold) OVER(component_partition), 0) as prev_tt_tier
    FROM tt_load_data
    WINDOW component_partition AS (PARTITION BY entity_id, scheme_id, travel_time_config_id ORDER BY tier)
  )

  , tt_aggregate_info_per_component as (
    select 
    entity_id
    , travel_time_config_id AS component_id
    , "Travel Time" as tier_type
    , COUNT(DISTINCT scheme_id) as n_schemes
    , MAX(tier) + 1 as n_tiers
    , MAX(travel_time_threshold)  as component_range
    , APPROX_QUANTILES(tt_diff_between_tiers, 100)[OFFSET(50)] as median_tier_range
    , MIN(tt_diff_between_tiers) as min_tier_tt_range
    , MAX(tt_diff_between_tiers) as max_tier_tt_range
    from tt_add_diff_between_tier
    -- where entity_id = "AP_PA"
    -- and asa_id = 1
    -- AND scheme_id = 92
    GROUP BY 1,2
    order by entity_id
  )

  , tt_load_usage_data as (
    SELECT 
    scheme_component_ids.travel_time_config_id
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
    "Travel Time" as component
    , a.*
    , IFNULL(b.n_asa,0) as n_asa
    , IFNULL(b.n_vendors,0) as n_vendors
    from tt_aggregate_info_per_component a
    LEFT JOIN tt_load_usage_data b
      ON a.component_id = b.travel_time_config_id
      AND a.entity_id = b.entity_id
  )

  select * from tt_add_usage_data;
###########################################################################################################################

#################################################### MOV METRICS ####################################################
  CREATE OR REPLACE TEMP TABLE mov_metrics as 
    with mov_load_data as (
      SELECT 
          entity_id
        , a.scheme_id
        , scheme_price_mechanisms
        , scheme_component_ids.mov_config_id
        , scheme_component_configs.mov_config

      FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2` a
      INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` tt_dl
        ON a.scheme_id = tt_dl.scheme_id
        AND a.entity_id = tt_dl.global_entity_id
        AND deleted = FALSE
      WHERE scheme_active_to IS NULL
      AND scheme_component_ids.mov_config_id IS NOT NULL
    )
    
    , mov_delay_data as (
      SELECT DISTINCT
          entity_id
        , a.scheme_id
        , mov_config_id
        , delay_config.delay_threshold

      FROM mov_load_data a
      LEFT JOIN UNNEST(mov_config) ttc
      LEFT JOIN UNNEST(surge_mov_row_config) delay_config
      WHERE scheme_price_mechanisms.is_surge_mov
    )

    , mov_add_delay_diff as (
      SELECT *
      , delay_threshold - IFNULL(LAG(delay_threshold) OVER(component_partition), 0) as tt_diff_between_delay_tiers
      , ROW_NUMBER() OVER(component_partition) AS delay_tier
      FROM mov_delay_data
      WINDOW component_partition AS (PARTITION BY entity_id, scheme_id, mov_config_id ORDER BY delay_threshold NULLS LAST)

    )

    , mov_aggregate_info_per_delay_tier as (
      select 
      entity_id
      , mov_config_id
      , "Delay Time" as tier_type
      , COUNT(DISTINCT scheme_id) as n_schemes
      , MAX(delay_tier) + 1 as n_delay_tiers
      , MAX(delay_threshold)  as delay_range
      , APPROX_QUANTILES(tt_diff_between_delay_tiers, 100)[OFFSET(50)] as median_delay_tier_range
      , MIN(tt_diff_between_delay_tiers) as min_delay_tier_range
      , MAX(tt_diff_between_delay_tiers) as max_delay_tier_range
      from mov_add_delay_diff
      GROUP BY 1,2,3
      -- order by entity_id
    )

    , mov_tt_data as (
      SELECT 
          entity_id
        , a.scheme_id
        , mov_config_id
        , ttc.*
        , tier

      FROM mov_load_data a
      LEFT JOIN UNNEST(mov_config) ttc
        WITH OFFSET AS tier
    )

    , mov_add_tt_diff as (
      SELECT *
        , travel_time_threshold - IFNULL(LAG(travel_time_threshold) OVER(component_partition), 0) as tt_diff_between_tiers
      FROM mov_tt_data
      WINDOW component_partition AS (PARTITION BY entity_id, mov_config_id, scheme_id ORDER BY tier)

    )

      , mov_aggregate_info_per_tt_tier as (
      select 
      entity_id
      , mov_config_id
      , "Travel Time" as tier_type
      , COUNT(DISTINCT scheme_id) as n_schemes
      , MAX(tier) + 1 as n_tiers
      , MAX(travel_time_threshold)  as tt_range
      , APPROX_QUANTILES(tt_diff_between_tiers, 100)[OFFSET(50)] as median_tier_range
      , MIN(tt_diff_between_tiers) as min_tier_range
      , MAX(tt_diff_between_tiers) as max_tier_range
      from mov_add_tt_diff
      GROUP BY 1,2,3
      -- order by entity_id
    )

    , mov_union_tier_type as (
      SELECT *
      FROM mov_aggregate_info_per_tt_tier

      UNION ALL 
      SELECT *
      FROM mov_aggregate_info_per_delay_tier
    )

    , mov_load_usage_data as (
      SELECT 
      scheme_component_ids.mov_config_id
      , entity_id
      , COUNT(DISTINCT vendor_code) as n_vendors
      , COUNT(DISTINCT asa_id) as n_asa
      FROM `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` 
      LEFT JOIN UNNEST(dps_asa_configuration_history) dps
      LEFT JOIN UNNEST(asa_price_config) 
      WHERE active_to is null
      GROUP BY 1,2
    )

    , mov_add_usage_data as (
      SELECT
      "MOV" as component
      , a.*
      , IFNULL(b.n_asa,0) as n_asa
      , IFNULL(b.n_vendors,0) as n_vendors
      from mov_union_tier_type a
      LEFT JOIN mov_load_usage_data b
        ON a.mov_config_id = b.mov_config_id
        AND a.entity_id = b.entity_id
    )


    select *
    from mov_add_usage_data
    WHERE TRUE;
###########################################################################################################################

#################################################### FLEET DELAY METRICS ####################################################

  CREATE OR REPLACE TEMP TABLE fleet_delay_metrics AS 
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
    from tt_add_usage_data;
###########################################################################################################################

#################################################### BASKET VALUE METRICS ####################################################

    CREATE OR REPLACE TEMP TABLE basket_value_metrics AS 
    with bvd_load_data as (
    SELECT 
        entity_id
      , a.scheme_id
      , scheme_component_ids.basket_value_config_id
      , ttc.*
      , tier

    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2` a
    LEFT JOIN UNNEST(scheme_component_configs.basket_value_config) ttc
      WITH OFFSET as tier
    INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` tt_dl
      ON a.scheme_id = tt_dl.scheme_id
      AND a.entity_id = tt_dl.global_entity_id
      AND deleted = FALSE
    WHERE scheme_active_to IS NULL
    AND scheme_component_ids.basket_value_config_id is not null
  )

  , bvd_add_diff_between_tier as (
    SELECT *
    , basket_value_threshold - IFNULL(LAG(basket_value_threshold) OVER(component_partition), 0) as tt_diff_between_tiers
    FROM bvd_load_data
    WINDOW component_partition AS (PARTITION BY entity_id, scheme_id, basket_value_config_id ORDER BY tier)
  )

  , bvd_aggregate_info_per_component as (
    select 
    entity_id
    , basket_value_config_id
    , "Food Value" as tier_type
    , COUNT(DISTINCT scheme_id) as n_schemes
    , MAX(tier) + 1 as n_tiers
    , MAX(basket_value_threshold)  as travel_time_range
    , APPROX_QUANTILES(tt_diff_between_tiers, 100)[OFFSET(50)] as median_tier_range
    , MIN(tt_diff_between_tiers) as min_tier_tt_range
    , MAX(tt_diff_between_tiers) as max_tier_tt_range
    from bvd_add_diff_between_tier
    -- where entity_id = "AP_PA"
    -- and asa_id = 1
    -- AND scheme_id = 92
    GROUP BY 1,2
    order by entity_id
  )

  , bvd_load_usage_data as (
    SELECT 
    scheme_component_ids.basket_value_config_id
    , entity_id
    , COUNT(DISTINCT vendor_code) as n_vendors
    , COUNT(DISTINCT asa_id) as n_asa
    FROM `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` 
    LEFT JOIN UNNEST(dps_asa_configuration_history) dps
    LEFT JOIN UNNEST(asa_price_config) 
    WHERE active_to is null
    GROUP BY 1,2
  )

  , bvd_add_usage_data as (
    SELECT
    "Basket Value" as component
    , a.*
    , IFNULL(b.n_asa,0) as n_asa
    , IFNULL(b.n_vendors,0) as n_vendors
    from bvd_aggregate_info_per_component a
    LEFT JOIN bvd_load_usage_data b
      ON a.basket_value_config_id = b.basket_value_config_id
      AND a.entity_id = b.entity_id
  )

  select * from bvd_add_usage_data;
###########################################################################################################################


#################################################### SERVICE FEE METRICS ####################################################
  CREATE OR REPLACE TEMP TABLE service_fee_metrics AS
  with sf_load_data as (
    SELECT 
        entity_id
      , a.scheme_id
      , scheme_price_mechanisms.service_fee_type
      , scheme_component_ids.service_fee_config_id
      , scheme_component_configs.service_fee_config

    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2` a
    -- LEFT JOIN UNNEST(scheme_component_configs.travel_time_config) ttc
    --   WITH OFFSET as tier
    INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` tt_dl
      ON a.scheme_id = tt_dl.scheme_id
      AND a.entity_id = tt_dl.global_entity_id
      AND deleted = FALSE
    WHERE scheme_active_to IS NULL
    AND scheme_component_ids.service_fee_config_id is not null
  )


  , sf_aggregate_component_info_type as (
    select 
    entity_id
    , service_fee_config_id
    , service_fee_type as tier_type
    , COUNT(DISTINCT scheme_id) as n_schemes
    , NULL as n_tiers
    , ANY_VALUE(service_fee_config.service_fee) AS component_range
    -- , APPROX_QUANTILES(service_fee_config.service_fee, 100)[OFFSET(50)]  as travel_time_range
    , NULL as median_tier_range
    , NULL as min_tier_tt_range
    , NULL as max_tier_tt_range
    from sf_load_data
    GROUP BY 1,2,3
    order by entity_id
  )

  , sf_load_usage_data as (
    SELECT 
    scheme_component_ids.service_fee_config_id
    , entity_id
    , COUNT(DISTINCT vendor_code) as n_vendors
    , COUNT(DISTINCT asa_id) as n_asa
    FROM `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` 
    LEFT JOIN UNNEST(dps_asa_configuration_history) dps
    LEFT JOIN UNNEST(asa_price_config) 
    WHERE active_to is null
    GROUP BY 1,2
  )

  , sf_add_usage_data as (
    SELECT
    "Service Fee" as component
    , a.*
    , IFNULL(b.n_asa,0) as n_asa
    , IFNULL(b.n_vendors,0) as n_vendors
    from sf_aggregate_component_info_type a
    LEFT JOIN sf_load_usage_data b
      ON a.service_fee_config_id = b.service_fee_config_id
      AND a.entity_id = b.entity_id
  )

  select * from sf_add_usage_data;

###########################################################################################################################

#################################################### COMPONENT METRICS ####################################################
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.component_metrics_usage` as
  with union_component_metrics as (
    SELECT *
    FROM travel_time_metrics

    UNION ALL

    SELECT *
    FROM mov_metrics

    UNION ALL

    SELECT *
    FROM fleet_delay_metrics

    UNION ALL

    SELECT *
    FROM basket_value_metrics

    UNION ALL 

    SELECT *
    FROM service_fee_metrics
  )

  , load_entity_data as (
    SELECT DISTINCT
    region
    , p.entity_id
    , IF(CONTAINS_SUBSTR(p.entity_id, "ODR"), "LaaS", "DH Business") as entity_type
    FROM `fulfillment-dwh-production.cl.countries` 
    LEFT JOIN UNNEST(platforms) p
    WHERE p.is_active
    ORDER BY 1,2
  )

  SELECT
  b.*
  , a.* EXCEPT(entity_id)
  FROM union_component_metrics a
  INNER JOIN load_entity_data b
    ON a.entity_id = b.entity_id