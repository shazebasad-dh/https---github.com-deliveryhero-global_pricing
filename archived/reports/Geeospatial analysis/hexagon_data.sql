## Scheduled here: https://console.cloud.google.com/bigquery/scheduled-queries/locations/us/configs/640711c5-0000-2eaf-b6ff-001a114f71b6/runs?authuser=0&project=logistics-data-staging-flat

create or replace table 
  `dh-logistics-product-ops.pricing.hexagon_data`
partition by 
    created_date
cluster by
    global_entity_id, city_name 
options
    (partition_expiration_days = null)
as
with
dps_entities as (
  select
    global_entity_id,
    global_entity_id entity_id,
    country_code,
  from `fulfillment-dwh-production.dl.dynamic_pricing_global_configuration`
  -- where global_entity_id = 'PY_AR'
),
cities as (
  select
      p.entity_id,
    --   co.country_code,
    --   co.country_iso,
      ci.name city_name,
    --   ci.id city_id,
      -- Get all H3 indexes within a geographic polygon
      jslibs.h3.ST_H3_POLYFILLFROMGEOG(
          -- Union all the zone shapes of a city into a single multipolygon
          st_union_agg(zo.shape),
        -- Define the H3 resolution
           9) area
  from `fulfillment-dwh-production.curated_data_shared.countries` co
  left join unnest(co.platforms) p
  inner join dps_entities dps using(entity_id, country_code)
  left join unnest(co.cities) ci
  left join unnest(ci.zones) zo
  where true
      and ci.is_active
      and zo.is_active
      and p.is_active
      -- and ci.name = 'Buenos aires'
      -- and country_code = 'ar'
    --   and zo.name = 'Palermo'
group by 1,2--,3,4,5
),
hexagons as (
    select
        entity_id,
        city_name,
        h3,
        -- Get the center of each h3
        `carto-os.carto.H3_CENTER`(h3) center,
        -- Create a circular polygon around h3
        ST_BUFFERWITHTOLERANCE(
            -- Get the center of each h3
            `carto-os.carto.H3_CENTER`(h3),
        -- Define the radius size: 1000m ~ 5.7x h3 resolution 9 edge size (https://h3geo.org/docs/core-library/restable/)
        1000,50) polygon_1km,
    from cities, unnest(area) h3
),
loved_brands as (
    select distinct
        entity_id as global_entity_id,
        cast(vendor_code as string) vendor_id
    from `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
    inner join dps_entities dps using (entity_id)
    where is_lb_lm = "Y"
    qualify update_timestamp = max(update_timestamp) over ()
),
-- select ST_BUFFERWITHTOLERANCE(`carto-os.carto.H3_CENTER`('89c2e310acbffff'),1744,50)
orders_data as (
    select
        o.global_entity_id,
        o.order_id,
        o.delivery_location.h3_level_9_index as h3,
        lb.vendor_id is not null as lb_order,
        o.value.gbv_local gbv,
        dps.delivery_costs_local cpo,
        o.value.delivery_fee_local + o.value.service_fee_local + o.value.mov_customer_fee_local as cf,
        o.value.delivery_fee_local + o.value.service_fee_local + o.value.commission_local + o.value.mov_customer_fee_local + o.value.joker_vendor_fee_local as take_in,
        o.value.voucher_dh_local + o.value.discount_dh_local incentives,
        st_geogpoint(o.delivery_location.longitude, o.delivery_location.latitude) as delivery_coordinates,
    from `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
    inner join dps_entities e using(global_entity_id)
    left join loved_brands lb using (global_entity_id, vendor_id)
    inner join `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders_v2` dps on dps.entity_id = o.global_entity_id and dps.platform_order_code = o.order_id
    where date(o.placed_at_local) >= current_date() - 30
        and o.is_sent
        and not o.is_qcommerce
        and not o.is_preorder
        and o.is_own_delivery
        and dps.delivery_costs_local + o.value.delivery_fee_local + o.value.service_fee_local + o.value.commission_local + o.value.mov_customer_fee_local + o.value.joker_vendor_fee_local is not null
),
hexagon_data as (
    select
        o.global_entity_id,
        o.order_id,
        h.city_name,
        h.h3,
        h.h3 = o.h3 hex_order,
        o.lb_order,
        o.gbv,
        o.cf,
        o.take_in,
        o.cpo,
        o.incentives,
        o.take_in - o.cpo gpo,
        o.take_in - o.cpo - o.incentives flgpo,
        -- any_value(h.center) center,
        h.polygon_1km,
        (1000 - st_distance(h.center,o.delivery_coordinates)) / 1000 as order_weight,
        st_distance(h.center,o.delivery_coordinates) distance,
    from orders_data o
    cross join hexagons h -- on st_contains(polygon_1744m, o.delivery_coordinates)
    where st_distance(h.center,o.delivery_coordinates) <= 1000
        -- and h.h3 = '88c2e311ddfffff'
)
select
    global_entity_id,
    city_name,
    h3,
    current_date created_date,
    sum(1) orders_considered,
    sum(order_weight) order_weight,
    sum(case when hex_order then 1 end) hex_orders,

    sum(case when hex_order and lb_order then 1 end) hex_lb_orders,
    safe_divide(sum(case when lb_order then order_weight end),sum(order_weight)) weighted_lb_share,
    safe_divide(sum(case when hex_order and lb_order then 1 end),sum(case when hex_order then 1 end)) hex_lb_share,

    safe_divide(sum(gbv * order_weight),sum(order_weight)) weighted_afv,
    safe_divide(sum(case when hex_order then gbv end),sum(case when hex_order then 1 end)) hex_afv,
    safe_divide(sum(case when hex_order and lb_order then gbv end),sum(case when hex_order then 1 end)) hex_lb_afv,
    
    safe_divide(sum(cpo * order_weight),sum(order_weight)) weighted_cpo,
    safe_divide(sum(case when hex_order then cpo end),sum(case when hex_order then 1 end)) hex_cpo,
    safe_divide(sum(case when hex_order and lb_order then cpo end),sum(case when hex_order then 1 end)) hex_lb_cpo,
    
    safe_divide(sum(take_in * order_weight),sum(order_weight)) weighted_take_in,
    safe_divide(sum(case when hex_order then take_in end),sum(case when hex_order then 1 end)) hex_take_in,
    safe_divide(sum(case when hex_order and lb_order then take_in end),sum(case when hex_order then 1 end)) hex_lb_take_in,
    
    safe_divide(sum(gpo * order_weight),sum(order_weight)) weighted_gpo,
    safe_divide(sum(case when hex_order then gpo end),sum(case when hex_order then 1 end)) hex_gpo,
    safe_divide(sum(case when hex_order and lb_order then gpo end),sum(case when hex_order then 1 end)) hex_lb_gpo,
    
    safe_divide(sum(flgpo * order_weight),sum(order_weight)) weighted_flgpo,
    safe_divide(sum(case when hex_order then flgpo end),sum(case when hex_order then 1 end)) hex_flgpo,
    safe_divide(sum(case when hex_order and lb_order then flgpo end),sum(case when hex_order then 1 end)) hex_lb_flgpo,
    
    safe_divide(sum(cf * order_weight),sum(gbv * order_weight)) weighted_cf_share,
    safe_divide(sum(take_in * order_weight),sum(gbv * order_weight)) weighted_take_in_share,
    safe_divide(sum(cpo * order_weight),sum(gbv * order_weight)) weighted_cpo_share,
    safe_divide(sum(incentives * order_weight),sum(gbv * order_weight)) weighted_incentive_share,
    safe_divide(sum(gpo * order_weight),sum(gbv * order_weight)) weighted_gpo_share,
    safe_divide(sum(flgpo * order_weight),sum(gbv * order_weight)) weighted_flgpo_share,

    -- any_value(polygon_1km) polygon_1km,
    -- safe_divide(sum(distance * order_weight),sum(order_weight)) weighted_distance,
    -- max(distance) max_distance,
    -- min(distance) min_distance,
from hexagon_data
group by 1,2,3
