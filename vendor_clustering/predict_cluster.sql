select * except(nearest_centroids_distance)
from ml.predict( 
    model `dh-logistics-product-ops.pricing.vendor_clustering_model_ar_food_afv_distance_custom_c5`,
    (
with
cities as (
  select
      p.entity_id,
      ci.name city_name,
      st_union_agg(zo.shape) shape,
  from `fulfillment-dwh-production.cl.countries` co
  left join unnest(co.platforms) p
  left join unnest(co.cities) ci
  left join unnest(ci.zones) zo
  where true
    and ci.is_active
    and p.is_active
    and zo.is_active
    and country_code = 'ar'
  group by 1,2
)
, vendors as (
  select
    v.global_entity_id,
    v.vendor_id,
    c.city_name,
    st_geogpoint(v.location.longitude, v.location.latitude) vendor_location,
  from `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v
  inner join `fulfillment-dwh-production.cl.vendors_v2` v2 
    on v.global_entity_id = v2.entity_id and v.vendor_id = v2.vendor_code
  inner join cities c 
    on v.global_entity_id = c.entity_id and st_contains(c.shape, st_geogpoint(v.location.longitude, v.location.latitude))
  where true
    -- and v.vertical_parent = 'Food'
    and v.vertical_type = 'restaurants'
    and v.global_entity_id = 'PY_AR'
    and not ('FastFood-AR' in unnest(v2.tags) 
      or v.vendor_id in ('190757', '191419', '191412', '191411', '191408', '191439', '311004', '391493')
      or v.vendor_id in ('388770', '389866', '389505')
      or 'Concepts-Ar' in unnest(v2.tags)
      or date(v.activation_date_local) between current_date() - 29 and current_date - 2)
)
, orders as (
  select
    o.global_entity_id,
    o.vendor_id,
    o.value.gbv_eur,
    v.city_name,
    st_distance(
      v.vendor_location,
      st_geogpoint(o.delivery_location.longitude, o.delivery_location.latitude)
    ) distance,
  from `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
  inner join vendors v using (global_entity_id, vendor_id)
  where date(o.placed_at_local) between current_date() - 29 and current_date() - 2
    and o.global_entity_id = 'PY_AR'
    and o.is_sent
    and o.is_own_delivery
)
,
grouped_city as (
  select 
    city_name,
    case when order_share >= 0.2 or sum(order_share) over (order by order_share desc) <= 0.8 then city_name else 'Other' end city_grouped,
  from (
  select
    city_name,
      sum(count(*)) over (partition by city_name) / sum(count(*)) over () order_share
  from orders
  group by 1
  )
  order by order_share desc
)
, aggregated_kpis as (
  select
    o.global_entity_id,
    g.city_grouped city_name,
    avg(o.gbv_eur) afv,
    avg(distance) avg_distance,
  from orders o
  left join grouped_city g using (city_name)
  group by 1,2,vendor_id
)
, percentiles as (
select
  global_entity_id,
  approx_quantiles(afv, 100) p_afv,
  approx_quantiles(avg_distance, 100) p_avg_distance,
from aggregated_kpis
group by 1)
select
  global_entity_id,
  city_name,
  afv,
  avg_distance,
from aggregated_kpis
where afv < (select p_afv[offset(99)] from percentiles)
  and avg_distance < (select p_avg_distance[offset(99)] from percentiles)
)
)
order by centroid_id
