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
,

 vendors as (
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
    and v.vertical_parent = 'Food'
    and v.vertical_type = 'restaurants'
    and v.global_entity_id = 'PY_AR'
    and not ('FastFood-AR' in unnest(v2.tags) 
      or v.vendor_id in ('190757', '191419', '191412', '191411', '191408', '191439', '311004', '391493')
      or v.vendor_id in ('388770', '389866', '389505')
      or 'Concepts-Ar' in unnest(v2.tags)
      or date(v.activation_date_local) between current_date() - 29 and current_date - 2)
),
orders as (
  select 
  *,
  percent_rank() over (partition by vendor_id order by distance asc) p_dist
  from (
  select
    v.global_entity_id,
    v.vendor_id,
    o.value.gbv_eur gbv,
    v.city_name,
    ###ADD MORE KPIs HERE
    o.value.delivery_fee_eur+o.value.service_fee_eur + o.value.mov_customer_fee_eur as cf,
    o.value.delivery_fee_eur + o.value.service_fee_eur + o.value.commission_eur + o.value.mov_customer_fee_eur + o.value.joker_vendor_fee_eur as take_in,
        o.value.voucher_dh_eur + o.value.discount_dh_eur incentives, 
    cast(st_distance(
      v.vendor_location,
      st_geogpoint(o.delivery_location.longitude, o.delivery_location.latitude)
    ) as INT64) distance,
    percent_rank() over (partition by v.vendor_id order by o.value.gbv_eur asc) p_afv,
  from vendors v
  inner join`fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
  on v.global_entity_id=o.global_entity_id and v.vendor_id = o.vendor_id and date(o.placed_at_local) between current_date() - 29 and current_date() - 2
    and o.global_entity_id = 'PY_AR'
    and o.is_sent
    and o.is_own_delivery
    group by 1,2,3,4,5,6,7,8 
    
    ##Remover ordenes outliers BV
    ##Remove ordenes outliers for distance
    ##Remove orders usando having
)
where
p_afv <0.99)
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
),
 aggregated_kpis as (
    SELECT
    DISTINCT
    global_entity_id,
    vendor_id,
    g.city_grouped city_name,
    count(vendor_id) orders,
    avg(gbv) afv,
    avg(distance) avg_distance
    from orders
    left join grouped_city g using (city_name)
    where p_dist<0.99
    Group by 1,2,3--,4
    having count(*) >=10),

normalized_kpis as (
select 
*,
percent_rank() over (partition by global_entity_id order by afv asc) p_afv,
percent_rank() over (partition by global_entity_id order by avg_distance asc) p_dist,
((afv - AVG(afv) OVER ()) / 
         NULLIF(STDDEV_POP(afv) OVER (), 0) 
       ) AS afv_normalized,
((avg_distance - AVG(avg_distance) OVER ()) / 
         NULLIF(STDDEV_POP(avg_distance) OVER (), 0) 
       ) AS distance_normalized       
from aggregated_kpis
)

select
*,
CASE when POW(POW(ABS(p_afv-0.5), 2) + POW(ABS(p_dist-0.5),2),0.5) < POW(POW(1,2)*0.2/3.14159,0.5)  then  'Cluster 1'
when ABS(p_afv)>0.5 and ABS(p_dist)>0.5 then 'Cluster 5'
when ABS(p_afv)<0.5 and ABS(p_dist)>0.5 then 'Cluster 4'
when ABS(p_afv)>0.5 and ABS(p_dist)<0.5 then 'Cluster 3'
when ABS(p_afv)<0.5 and ABS(p_dist)<0.5 then 'Cluster 2'
end as centroid_id
from normalized_kpis
Order by centroid_id asc
