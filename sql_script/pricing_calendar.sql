create or replace table `dh-logistics-product-ops.pricing.pricing_calendar` as
with
blocks as (
    select
    co.region,
    p.entity_id,
    co.country_code,
    ci.id city_id,
    ci.name city_name,
    st_clusterdbscan(zo.shape,0,1) over (partition by entity_id, co.country_code order by zo.id) + 1 block_id,
    zo.id zone_id,
    lower(zo.name) zone_name,
    st_centroid(zo.shape) coordinates,
  from `fulfillment-dwh-production.cl.countries` co
  left join unnest(co.platforms) p
  left join unnest(co.cities) ci
  left join unnest(ci.zones) zo
  inner join `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities` g on p.entity_id = g.global_entity_id
  where true
    and ci.is_active
    and zo.is_active
    and g.is_reporting_enabled
    -- and region = "Americas"
)
, dps_experiments as (
  select distinct
    s.entity_id,
    s.country_code,
    s.test_name,
    (s.test_start_date) start_date,
    (s.test_end_date) end_date,
    case split (upper(s.test_name), '_')[safe_offset(4)]
      when 'O' then 'Orders'
      when 'P' then 'Profit'
      when 'R' then 'Revenue'
    end objective,
    initcap(vp) vertical_parent,
    -- case split (upper(s.test_name), '_')[safe_offset(2)]
    --   when 'R' then 'Restaurants'
    --   when 'D' then 'Dmarts'
    --   when 'G' then 'Groceries'
    --   when 'C' then 'Coffee'
    --   when 'L' then 'Local Stores'
    --   when 'A' then 'Any other vertical'
    --   when 'Z' then 'Mix'
    -- end vertical,
    case left(split (upper(s.test_name), '_')[safe_offset(3)],1)
      when 'A' then 'Flat DF'
      when 'B' then 'DBDF'
      when 'C' then 'Flat MOV'
      when 'D' then 'Variable MOV'
      when 'E' then 'Fleet Delay MOV'
      when 'F' then 'Fleet Delay DF'
      when 'G' then 'BBDF or BVDF'
      when 'H' then 'Service Fee'
      when 'I' then 'FDNC'
      when 'J' then 'Time'
      when 'S' then 'Small Basket Fee'
      when '0' then 'No mechanism'
      when 'Z' then 'Other feature'
    end main_feature,
    case right(split (upper(s.test_name), '_')[safe_offset(3)],1)
      when 'A' then 'Flat DF'
      when 'B' then 'DBDF'
      when 'C' then 'Flat MOV'
      when 'D' then 'Variable MOV'
      when 'E' then 'Fleet Delay MOV'
      when 'F' then 'Fleet Delay DF'
      when 'G' then 'BBDF or BVDF'
      when 'H' then 'Service Fee'
      when 'I' then 'FDNC'
      when 'J' then 'Time'
      when 'S' then 'Small Basket Fee'
      when '0' then 'No mechanism'
      when 'Z' then 'Other feature'
    end other_features,
    lower(zone_name) zone_name,
  from `fulfillment-dwh-production.cl.dps_experiment_setups` s, unnest(zone_ids) zone_id, unnest(test_vertical_parents) vp
  left join blocks b using(entity_id, country_code, zone_id)
  where not misconfigured
    -- and region = "Americas"
    and test_start_date is not null
    -- and test_name = 'UY_20221011_R_B0_O_Elasticity'
)
, planned_experiments as (
  select
    c.entity_id,
    lower(right(c.entity_id,2)) country_code,
    c.test_name,
    timestamp(c.start_date, "America/Buenos_Aires") as start_date,
    timestamp(c.end_date, "America/Buenos_Aires") as end_date,
    case split (upper(c.test_name), '_')[safe_offset(4)]
      when 'O' then 'Orders'
      when 'P' then 'Profit'
      when 'R' then 'Revenue'
    end objective,
    initcap(c.vertical_parent) vertical_parent,
    -- case split (upper(c.test_name), '_')[safe_offset(2)]
    --   when 'R' then 'Restaurants'
    --   when 'D' then 'Dmarts'
    --   when 'G' then 'Groceries'
    --   when 'C' then 'Coffee'
    --   when 'L' then 'Local Stores'
    --   when 'A' then 'Any other vertical'
    --   when 'Z' then 'Mix'
    -- end vertical,
    case left(split (upper(c.test_name), '_')[safe_offset(3)],1)
      when 'A' then 'Flat DF'
      when 'B' then 'DBDF'
      when 'C' then 'Flat MOV'
      when 'D' then 'Variable MOV'
      when 'E' then 'Fleet Delay MOV'
      when 'F' then 'Fleet Delay DF'
      when 'G' then 'BBDF or BVDF'
      when 'H' then 'Service Fee'
      when 'I' then 'FDNC'
      when 'J' then 'Time'
      when 'S' then 'Small Basket Fee'
      when '0' then 'No mechanism'
      when 'Z' then 'Other feature'
    end main_feature,
    case right(split (upper(c.test_name), '_')[safe_offset(3)],1)
      when 'A' then 'Flat DF'
      when 'B' then 'DBDF'
      when 'C' then 'Flat MOV'
      when 'D' then 'Variable MOV'
      when 'E' then 'Fleet Delay MOV'
      when 'F' then 'Fleet Delay DF'
      when 'G' then 'BBDF or BVDF'
      when 'H' then 'Service Fee'
      when 'I' then 'FDNC'
      when 'J' then 'Time'
      when 'S' then 'Small Basket Fee'
      when '0' then 'No mechanism'
      when 'Z' then 'Other feature'
    end other_features,
    lower(trim(z)) zone_name,
  from `dh-logistics-product-ops.pricing.latam_ab_tests_calendar` c
  left join unnest(split(replace(zones,"+",","),",")) z
  -- where test_name = 'UY_20221011_R_B0_O_Elasticity'
)
, experiments_calendar as (
  select
    ifnull(t.entity_id, p.entity_id) entity_id,
    ifnull(t.country_code, p.country_code) country_code,
    ifnull(t.test_name, p.test_name) test_name,
    ifnull(t.start_date, p.start_date) start_date,
    ifnull(t.end_date, p.end_date) end_date,
    ifnull(t.objective, p.objective) objective,
    ifnull(t.vertical_parent, p.vertical_parent) vertical_parent,
    -- ifnull(t.vertical, p.vertical) vertical,
    ifnull(t.main_feature, p.main_feature) main_feature,
    ifnull(t.other_features, p.other_features) other_feature,
    ifnull(t.zone_name, p.zone_name) zone_name,
  from dps_experiments t
  left join planned_experiments p using (entity_id, country_code, test_name, zone_name)
)
, experiments_to_blocks as (
  select
    z.entity_id,
    z.country_code,
    z.test_name,
    z.objective,
    z.vertical_parent,
    -- min(z.vertical) vertical,
    z.main_feature,
    z.other_feature,
    b.block_id,
    min(z.start_date) start_date,
    min(z.end_date) end_date,
  from experiments_calendar z
  inner join blocks b using(entity_id, country_code, zone_name)
  group by 1,2,3,4,5,6,7,8
)
, vendor_vertical_parent as (
  select
    content.global_entity_id,
    content.vendor_id,
    initcap(content.vertical_parent) vertical_parent,
    -- content.vertical_type,
  from `fulfillment-dwh-production.curated_data_shared_data_stream.vendor_stream` s
  qualify row_number() over (partition by s.content.global_entity_id, s.content.vendor_id order by s.content.timestamp desc) = 1
)
, vendors_to_zones as (
  select
    s.global_entity_id,
    z.country_code,
    s.vendor_id as vendor_code,
    s.vertical_parent,
    -- s.vertical_type,
    z.id as zone_id,
  from vendor_vertical_parent s
  left join `fulfillment-dwh-production.cl.vendors_v2` v
    on v.vendor_code = s.vendor_id and v.entity_id = s.global_entity_id
  left join unnest(zones) z
)
, dps_campaigns as (
  select
    global_entity_id entity_id,
    country_code,
    campaign_name,
    zone_id,
    vertical_parent,
    -- vertical_type as vertical,
    s.start_at as start_date,
    case when s.end_at <= ifnull(s.recurrence_end_at, s.end_at) then ifnull(s.recurrence_end_at, s.end_at) else s.end_at end as end_date,
    count(vendor_code) vendors,
  from `fulfillment-dwh-production.dl.dynamic_pricing_campaign_history` h
  left join unnest(vendor_ids) vendor_code
  left join `fulfillment-dwh-production.cl.campaign_dps_info` c using (global_entity_id, country_code, campaign_id)
  left join unnest(campaign_schedule) s
  left join vendors_to_zones v using (global_entity_id, country_code, vendor_code)
  where s.end_at is not null
    and date(s.start_at) >= "2022-06-01"
    and h.created_date >= "2022-05-01"
    and c.campaign_deleted_at is null
    -- and c.region = "Americas"
    -- and campaign_name = 'DF$0/MOV399 - COSTUMBRES ARGENTINAS 17-06-22 (11 a 15 h)'
  group by 1,2,3,4,5,6,7
  order by 1,2,3,4,5,6,7
)
, campaigns_to_blocks as (
  select
    c.entity_id,
    c.country_code,
    c.campaign_name,
    -- min(c.objective) objective,
    c.vertical_parent,
    -- min(z.vertical) vertical,
    -- min(c.main_feature) main_feature,
    -- min(c.other_feature) other_feature,
    b.block_id,
    min(c.start_date) start_date,
    min(c.end_date) end_date,
  from dps_campaigns c
  inner join blocks b using(entity_id, country_code, zone_id)
  group by 1,2,3,4,5
)
select
 'Experiment' type,
  b.region,
  b.entity_id,
  a.country_code,
  a.test_name name,
  a.start_date,
  a.end_date,
  a.objective,
  a.vertical_parent,
  a.main_feature,
  a.other_feature,
  b.block_id,
  b.city_id,
  b.city_name,
  b.zone_id,
  initcap(b.zone_name) zone_name,
  z.zone_name is not null is_targeted,
  st_x(b.coordinates) longitude,
  st_y(b.coordinates) latitude,
from blocks b
left join experiments_to_blocks a using (entity_id, country_code, block_id)
left join experiments_calendar z using (entity_id, country_code, zone_name, test_name)
qualify count(distinct a.test_name) over (partition by b.entity_id) > 0
union all
select
  'Campaign' type,
  b.region,
  a.entity_id,
  a.country_code,
  a.campaign_name name,
  a.start_date,
  a.end_date,
  cast(null as string) objective,
  a.vertical_parent,
  cast(null as string) main_feature,
  cast(null as string) other_feature,
  b.block_id,
  b.city_id,
  b.city_name,
  b.zone_id,
  initcap(b.zone_name) zone_name,
  z.zone_id is not null is_targeted,
  st_x(b.coordinates) longitude,
  st_y(b.coordinates) latitude,
from campaigns_to_blocks a
left join blocks b using (entity_id, country_code, block_id)
left join dps_campaigns z using(entity_id, country_code, zone_id, campaign_name)
qualify count(distinct a.campaign_name) over (partition by b.entity_id) > 0
