create or replace table `dh-logistics-product-ops.pricing.vendors_clustered` as
## add the other PY countries to the query
with
vendors as (
  select
    v.global_entity_id entity_id,
    v.vendor_id,
    v.vendor_name,
    v.location.longitude,
    v.location.latitude,
  ## how can we make the exception handling more scalable for more countries?
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
  left join `fulfillment-dwh-production.cl.vendors_v2` v2 
  on v.global_entity_id = v2.entity_id and v.vendor_id = v2.vendor_code 
  where true
    and v.vertical_type = 'restaurants'
    and v.global_entity_id in ('PY_AR', 'AP_PA', 'PY_UY', 'PY_BO', 'PY_CL', 'PY_EC', 'PY_PY', 'PY_PE', 'PY_VE', 'PY_GT', 'PY_CR', 'PY_SV', 'PY_HN', 'PY_NI', 'PY_DO')
    and v.is_online
)
,
orders as (
  select
    o.entity_id,
    o.vendor_id,
    o.city_name,
    o.gfv_eur gbv,
    o.linear_dist_customer_vendor,
    ifnull(o.delivery_fee_eur,0) + ifnull(o.service_fee_eur,0) + ifnull(o.mov_customer_fee_eur,0) cf,
    ifnull(o.commission_eur,0) + ifnull(o.joker_vendor_fee_eur,0) vf,
    ifnull(o.voucher_dh_eur,0) + ifnull(o.discount_dh_eur,0) dh_incentives,
    ifnull(o.voucher_other_eur,0) + ifnull(o.discount_other_eur,0) other_incentives,
    ifnull(o.delivery_costs_eur,0) cpo,
  from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2` o
  where true
    and o.created_date between current_date() - 29 and current_date() - 2
    --and o.entity_id = 'PY_AR'
    and o.region = 'Americas'
    and o.is_sent
    and o.is_own_delivery
  ## remove the subscribed orders?
  ## remove outliers: >= 5 std deviations above the median
  qualify o.gfv_eur < percentile_cont(o.gfv_eur, 0.5) over (partition by o.entity_id) + 5 * stddev_pop(o.gfv_eur) over (partition by o.entity_id)
  and o.linear_dist_customer_vendor < percentile_cont(o.linear_dist_customer_vendor, 0.5) over (partition by o.entity_id) + 5 * stddev_pop(o.linear_dist_customer_vendor) over (partition by o.entity_id)
)
,
grouped_city as (
  select
    entity_id,
    city_name,
    case
      when order_share >= 0.2 or sum(order_share) over (partition by entity_id order by order_share desc) <= 0.8 then city_name
      else 'Other'
    end city_grouped,
  from (
  select
    entity_id,
    city_name,
    sum(count(*)) over (partition by entity_id, city_name) / sum(count(*)) over (partition by entity_id) order_share
  from orders
  group by 1,2
  )
  order by entity_id, order_share desc
)
,
competition as (
  select distinct
    pt.entity_id,
    cast(partner_id as string) vendor_id,
  from `peya-bi-tools-pro.il_scraping.dim_competitor_historical` c
  left join `peya-bi-tools-pro.il_core.dim_partner` p
    on c.peya_partner_id = p.partner_id and c.country = p.country.country_name
  left join `fulfillment-dwh-production.cl.countries` cn
    on lower(p.country.country_code) = lower(cn.country_code)
  left join unnest(platforms) pt
  where pt.entity_id in ('PY_AR', 'AP_PA', 'PY_UY', 'PY_BO', 'PY_CL', 'PY_EC', 'PY_PY', 'PY_PE', 'PY_VE', 'PY_GT', 'PY_CR', 'PY_SV', 'PY_HN', 'PY_NI', 'PY_DO')
    and c.date between current_date() - 29 and current_date() - 2
    -- and c.peya_partner_id is not null
    and (rappi_partner_id is not null
      or ubereats_partner_id is not null)
  group by 1,2
)
,
 aggregated_kpis as (
  select
    entity_id,
    vendor_id,
    vendor_name,
    longitude,
    latitude,
    g.city_grouped city_name,
    exception,
    new_vendor,
    competition.vendor_id is not null is_mult_platform,
    count(*) < 10 insufficient_data,
    count(*) orders,
    avg(gbv) avg_basket,
    avg(linear_dist_customer_vendor) avg_distance,
    avg(cf) avg_cf,
    avg(vf) avg_vf,
    avg(dh_incentives) avg_dh_incentives,
    avg(other_incentives) avg_other_incentives,
    avg(cpo) avg_delivery_cost,
  from vendors
  left join competition
    using (entity_id, vendor_id)
  left join orders o
    using (entity_id, vendor_id)
  left join grouped_city g
    using (city_name, entity_id)
  group by 1,2,3,4,5,6,7,8,9
)
,
normalized_kpis as (
  select 
    *,
    percent_rank() over (partition by entity_id, city_name, exception, new_vendor, insufficient_data order by avg_basket asc) p_avg_basket,
    percent_rank() over (partition by entity_id, city_name, exception, new_vendor, insufficient_data order by avg_distance asc) p_avg_distance,
    ((avg_basket - avg(avg_basket) over(partition by entity_id, city_name, exception, new_vendor, insufficient_data)) 
      / nullif(stddev_pop(avg_basket) over (partition by entity_id, city_name, exception, new_vendor, insufficient_data), 0)) AS avg_basket_normalized,
    ((avg_distance - avg(avg_distance) over(partition by entity_id, city_name, exception, new_vendor, insufficient_data)) 
      / nullif(stddev_pop(avg_distance) over (partition by entity_id, city_name, exception, new_vendor, insufficient_data), 0)) AS avg_distance_normalized,
  from aggregated_kpis
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
  -- case
  --   when exception then 'Exception'
  --   when new_vendor then 'New Vendor'
  --   when insufficient_data then 'Insufficient data'
  --   # fits 30% of the vendors from the median in the central cluster
  --   when pow(pow(abs(p_avg_basket - 0.5), 2) + pow(abs(p_avg_distance - 0.5), 2), 0.5) < pow(pow(1, 2) * 0.3 / acos(-1), 0.5)  then  'Central cluster'
  --   when abs(p_avg_basket) >= 0.5 and abs(p_avg_distance) >= 0.5 then 'High basket, high distance'
  --   when abs(p_avg_basket) <= 0.5 and abs(p_avg_distance) >= 0.5 then 'Low basket, high distance'
  --   when abs(p_avg_basket) >= 0.5 and abs(p_avg_distance) <= 0.5 then 'High basket, low distance'
  --   when abs(p_avg_basket) <= 0.5 and abs(p_avg_distance) <= 0.5 then 'Low basket, low distance'
  -- end as cluster_2,
from normalized_kpis
order by cluster asc
