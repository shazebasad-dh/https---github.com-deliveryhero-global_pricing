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
  , travel_time_config_id
  , "Travel Time" as tier_type
  , COUNT(DISTINCT scheme_id) as n_schemes
  , MAX(tier) + 1 as n_tiers
  , MAX(travel_time_threshold)  as travel_time_range
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
    ON a.travel_time_config_id = b.travel_time_config_id
    AND a.entity_id = b.entity_id
)

select * from tt_add_usage_data