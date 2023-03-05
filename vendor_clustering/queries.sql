## to-do: create a DAG to orchestrate the update: https://github.com/omar-elmaria/airflow_at_delivery_hero
create or replace table `dh-logistics-product-ops.pricing.clustering_caps` as
select
  o.entity_id,
  avg(o.gfv_eur) avg_gfv_eur,
  stddev_pop(o.gfv_eur) stddev_pop_gfv_eur,
  -- avg(o.linear_dist_customer_vendor) avg_linear_dist_customer_vendor,
  -- stddev_pop(o.linear_dist_customer_vendor) stddev_pop_linear_dist_customer_vendor,
  avg(o.dps_travel_time) avg_dps_travel_time,
  stddev_pop(o.dps_travel_time) stddev_pop_dps_travel_time,
from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
where true
  and o.created_date between current_date() - 29 and current_date() - 2
  and o.is_sent
  and o.is_own_delivery
  and o.vertical_type = 'restaurants'
group by 1
;
create or replace table `dh-logistics-product-ops.pricing.clustering_orders` as
select
  o.entity_id,
  o.vendor_id,
  o.city_name,
  o.zone_name,
  o.gfv_eur gbv,
  -- o.linear_dist_customer_vendor,
  o.dps_travel_time,
  o.dps_travel_time_fee_local,
  ifnull(o.delivery_fee_eur,0) + ifnull(o.service_fee_eur,0) + ifnull(o.mov_customer_fee_eur,0) cf,
  ifnull(o.commission_eur,0) + ifnull(o.joker_vendor_fee_eur,0) vf,
  ifnull(o.voucher_dh_eur,0) + ifnull(o.discount_dh_eur,0) dh_incentives,
  ifnull(o.voucher_other_eur,0) + ifnull(o.discount_other_eur,0) other_incentives,
  ifnull(o.delivery_costs_eur,0) cpo,
from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
left join  `dh-logistics-product-ops.pricing.clustering_caps` c
  using (entity_id)
where true
## to-do: remove the subscribed orders
## to-do: remove orders from new customers
  and o.created_date between current_date() - 29 and current_date() - 2
  and o.is_sent
  and o.is_own_delivery
  and o.vertical_type = 'restaurants'
  and o.variant in ('Original', 'Control')
  and o.vendor_price_scheme_type in ('Experiment', 'Automatic scheme', 'Manual')
  # remove outliers (>= 5 std deviations above the median)
  and o.gfv_eur < avg_gfv_eur + 5 * stddev_pop_gfv_eur
  and o.dps_travel_time < c.avg_dps_travel_time + 5 * c.stddev_pop_dps_travel_time
  -- and o.linear_dist_customer_vendor < avg_linear_dist_customer_vendor + 5 * stddev_pop_linear_dist_customer_vendor  
  -- and o.region = 'Americas'
  -- and o.entity_id = 'PY_AR'
  -- and o.vendor_id = '144883'
;
create or replace table `dh-logistics-product-ops.pricing.clustering_vendor_areas` as
with
total_orders as (
  select
    entity_id,
    city_name,
    zone_name,
    vendor_id,
    count(*) orders,
  from `dh-logistics-product-ops.pricing.clustering_orders`
  group by 1,2,3,4
)
,
most_frequent_area as (
  select
    entity_id,
    vendor_id,
    orders,
    ifnull(nth_value(city_name, 1 ignore nulls) over a, lead(city_name, 1) over a) as city_name,
    ifnull(nth_value(zone_name, 1 ignore nulls) over a, lead(zone_name, 1) over a) as zone_name,
  from total_orders
  -- where vendor_id = '332154'
  -- and entity_id = 'PY_CL'
  window a as (partition by entity_id, vendor_id order by orders desc)
)
select
  entity_id,
  city_name,
  zone_name,
  vendor_id,
  sum(orders) vendor_orders,
from most_frequent_area
group by 1,2,3,4
;
create or replace table `dh-logistics-product-ops.pricing.clustering_areas` as
with
zone_metrics as (
  select
    entity_id,
    city_name,
    zone_name,
    sum(vendor_orders) zone_orders,
    sum(sum(vendor_orders)) over a city_orders,
    sum(sum(vendor_orders)) over b entity_orders,
    sum(vendor_orders) / sum(sum(vendor_orders)) over a zone_share_city,
    sum(sum(vendor_orders)) over a / sum(sum(vendor_orders)) over b city_share_entity,
    sum(vendor_orders) / sum(sum(vendor_orders)) over b zone_share_entity,
  from `dh-logistics-product-ops.pricing.clustering_vendor_areas`
  group by 1,2,3
  window
    a as (partition by entity_id, city_name),
    b as (partition by entity_id)
  order by 1,4 desc
)
,
city_share as (
  select
    entity_id,
    city_name,
    city_share_entity city_order_share,
    sum(zone_orders) city_orders,
    sum(min(city_share_entity)) over a city_order_share_cum,
  from zone_metrics
  group by 1,2,3
  window a as (partition by entity_id order by city_share_entity desc)
)
,
area_share as (
  select
    entity_id,
    city_name,
    city_order_share,
    city_order_share_cum,
    zone_name,
    zone_share_city zone_order_share,
    sum(zone_orders) zone_orders,
    sum(min(zone_share_city)) over a zone_order_share_cum,
  from zone_metrics
  left join city_share using (entity_id, city_name)
  group by 1,2,3,4,5,6
  window a as (partition by entity_id, city_name order by zone_share_city desc)
  order by city_order_share_cum, zone_order_share_cum
)
select
  entity_id,
  city_name,
  zone_name,
  case
  # if the city represents more than 50% of the total orders, segment it by zone
   when city_order_share > 0.5 and zone_order_share_cum < 0.8 then concat(city_name,' - ', zone_name)
    when city_order_share > 0.5 and zone_order_share between 0.2 and 0.8 then concat(city_name,' - ', zone_name)
  # otherwise, identify the major cities
    when city_order_share between 0.2 and 0.5 then city_name 
    when city_order_share_cum <= 0.8 then city_name
  # and group the smaller cities
    else 'Other'
  end area_grouped,
  -- city_order_share,
  -- city_order_share_cum,
  -- zone_order_share,
  -- zone_order_share_cum,
  -- zone_orders,
from area_share
order by entity_id, city_order_share_cum, zone_order_share_cum
;
create or replace table `dh-logistics-product-ops.pricing.clustering_vendors` as
with
vendors as (
  select
    g.segment region,
    g.management_entity,
    v.global_entity_id entity_id,
    v.vendor_id,
    v.vendor_name,
    v.location.longitude,
    v.location.latitude,
    a.city_name,
    a.zone_name,
  ## to-do: make the exception handling more scalable
    case global_entity_id 
    when 'PY_AR' then (
     'FastFood-AR' in unnest(v2.tags) 
      or v.vendor_id in ('190757', '191419', '191412', '191411', '191408', '191439', '311004', '391493')
      or v.vendor_id in ('388770', '389866', '389505')
      or 'Concepts-Ar' in unnest(v2.tags))
    when 'AP_PA' then ( 
      'no-dbdf-group-4' in unnest(v2.tags)
      or 'no-dbdf-group-3' in unnest(v2.tags))
    when 'PY_EC' then ( 
      'EC_High_Commission_Food_Feb' in unnest(v2.tags))
    when 'PY_BO' then ( 
      'CONCEPTS_BO_SCZ_FEB' in unnest(v2.tags)
      or 'CONCEPTS_BO_NOTSCZ_FEB' in unnest(v2.tags))
     end as exception,
    date(v.activation_date_local) between current_date() - 29 and current_date - 2 as new_vendor,
  from `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v
  left join `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities` g
    using (global_entity_id)
  left join `fulfillment-dwh-production.cl.vendors_v2` v2 
    on v.global_entity_id = v2.entity_id 
      and v.vendor_id = v2.vendor_code 
  left join `dh-logistics-product-ops.pricing.clustering_vendor_areas` a
    using (entity_id, vendor_id)
  where true
    and v.vertical_type = 'restaurants'
    -- and v.global_entity_id in ('PY_AR', 'AP_PA', 'PY_UY', 'PY_BO', 'PY_CL', 'PY_EC', 'PY_PY', 'PY_PE', 'PY_VE', 'PY_GT', 'PY_CR', 'PY_SV', 'PY_HN', 'PY_NI', 'PY_DO')
    and v.is_online
    and not v.is_test_vendor
    and g.is_reporting_enabled
    -- and a.city_name = 'Bangkok'
)
,
competition as (
  select
    cn.global_entity_id entity_id,
    cast(partner_id as string) vendor_id,
    min(rappi_partner_id) is not null or min(ubereats_partner_id) is not null as is_competed,
  from  `peya-bi-tools-pro.il_core.dim_partner` p
  left join `peya-bi-tools-pro.il_scraping.dim_competitor_historical` c
    on c.peya_partner_id = p.partner_id 
      and c.country = p.country.country_name
  left join `fulfillment-dwh-production.dl.dynamic_pricing_global_configuration` cn
    on lower(p.country.country_code) = lower(cn.country_code)
  where c.date between current_date() - 29 and current_date() - 2
  group by 1,2
)
,
asa_lb as (
  select
    entity_id,
    vendor_code vendor_id,
    asa_id,
    master_asa_id,
    asa_name,
    case is_lb_lm when 'Y' then true when 'N' then false else null end is_lb,
    cvr3,
    vendor_cvr3_slope,
    asa_cvr3_slope,
  from `dh-logistics-product-ops.pricing.final_vendor_list_all_data_loved_brands_scaled_code`
  qualify row_number() over a = 1
  window a as (partition by entity_id, vendor_id order by update_timestamp desc)
)
select
  v.region,
  v.management_entity,
  v.entity_id,
  v.vendor_id,
  v.vendor_name,
  v.longitude,
  v.latitude,
  v.exception,
  v.new_vendor,
  v.city_name,
  v.zone_name,
  c.is_competed,
  a.* except (entity_id, vendor_id),
from vendors v
left join competition c
  using (entity_id, vendor_id)
left join asa_lb a
  using (entity_id, vendor_id)
;
create or replace table `dh-logistics-product-ops.pricing.vendors_clustered` as
with
aggregated_kpis as (
  select
    v.* except (city_name, zone_name),
    g.area_grouped area_name,
    count(*) < 10 insufficient_data,
    count(*) orders,
    avg(o.gbv) avg_basket,
    avg(o.dps_travel_time) avg_distance,
    avg(o.cf) avg_cf,
    avg(o.vf) avg_vf,
    avg(o.dh_incentives) avg_dh_incentives,
    avg(o.other_incentives) avg_other_incentives,
    avg(o.cpo) avg_delivery_cost,
  from `dh-logistics-product-ops.pricing.clustering_vendors` v
  left join `dh-logistics-product-ops.pricing.clustering_areas` g
    using (entity_id, city_name, zone_name)
  left join `dh-logistics-product-ops.pricing.clustering_orders` o
    using (entity_id, vendor_id)
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
,
normalized_kpis as (
  select 
    *,
    percent_rank() over a p_avg_basket,
    percent_rank() over b p_avg_distance,
    ((avg_basket - avg(avg_basket) over c) / nullif(stddev_pop(avg_basket) over c, 0)) AS avg_basket_normalized,
    ((avg_distance - avg(avg_distance) over c) / nullif(stddev_pop(avg_distance) over c, 0)) AS avg_distance_normalized,
  from aggregated_kpis
  window
    a as (partition by entity_id, area_name, exception, new_vendor, insufficient_data order by avg_basket asc),
    b as (partition by entity_id, area_name, exception, new_vendor, insufficient_data order by avg_distance asc),
    c as (partition by entity_id, area_name, exception, new_vendor, insufficient_data)
)
select
  *,
  case
    when exception then 'Exception'
    when new_vendor then 'New Vendor'
    when insufficient_data then 'Insufficient data'
    # assign vendors within 2/3 standard deviations from the average in the central cluster
    when pow(pow(abs(avg_basket_normalized), 2) + pow(abs(avg_distance_normalized), 2), 0.5) < 2/3  then  'Central cluster'
    when avg_basket_normalized >= 0 and avg_distance_normalized >= 0 then 'High basket, high distance'
    when avg_basket_normalized <= 0 and avg_distance_normalized >= 0 then 'Low basket, high distance'
    when avg_basket_normalized >= 0 and avg_distance_normalized <= 0 then 'High basket, low distance'
    when avg_basket_normalized <= 0 and avg_distance_normalized <= 0 then 'Low basket, low distance'
    else 'Not defined'
  end as cluster,
from normalized_kpis
order by cluster asc
;
create or replace table `dh-logistics-product-ops.pricing.clusters_travel_time_mapped` as
with
shares as (
  select
    cast([01,05,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,99,100] as array<int64>) as share_per_tier, -- Cumulative shares selected
)
,
tiers as (
  select
    cum_share,
    row_number() over (order by cum_share) tier,
    ifnull(lag(cum_share) over (order by cum_share),0) lower_tt_percentile,
    cum_share upper_tt_percentile,
from shares, unnest(share_per_tier) as cum_share with offset offset
)
, 
orders as (
  select
    v.entity_id,
    v.area_name,
    v.cluster,
    o.dps_travel_time,
    o.dps_travel_time_fee_local,
    safe_divide(row_number() over (partition by v.entity_id, v.area_name, v.cluster order by o.dps_travel_time, o.dps_travel_time_fee_local),
      count(*) over (partition by v.entity_id, v.area_name, v.cluster)) * 100 tt_percentile,
  from `dh-logistics-product-ops.pricing.vendors_clustered` v
  inner join `dh-logistics-product-ops.pricing.clustering_orders` o using (entity_id, vendor_id)
  left join `dh-logistics-product-ops.pricing.clustering_caps` c using (entity_id)
--   where o.dps_travel_time < c.avg_dps_travel_time + 5 * c.stddev_pop_dps_travel_time
)
select
  o.entity_id,
  o.area_name,
  o.cluster,
  t.tier,
  t.upper_tt_percentile - t.lower_tt_percentile as share,
  t.cum_share,
  max(o.dps_travel_time) dps_travel_time_decimal,
  format_time("%M:%S", time(timestamp_seconds(cast(round(max(o.dps_travel_time) * 60) as int64)))) dps_travel_time_formatted,
  round(avg(o.dps_travel_time_fee_local),2) current_average_travel_time_fee_local,
  approx_quantiles(o.dps_travel_time_fee_local, 100)[offset(50)] median_tt_fee,
  count(*) orders,
  sum(count(*)) over (partition by o.entity_id, o.area_name, o.cluster) total_orders
from orders o
left join tiers t on o.tt_percentile > t.lower_tt_percentile and o.tt_percentile <= t.upper_tt_percentile
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6
;
