with
mov_components_with_sbf as (
select distinct
  region,
  global_entity_id,
  configuration_id mov_id,
  name mov_name,
  hard_mov,
  max_top_up,
  updated_by,
  updated_at,
from `fulfillment-dwh-production.dl.dynamic_pricing_mov_configuration`
where not deleted
  and flexible
order by 1,2,3
)
,
schemes_with_sbf as (
  select
    s.global_entity_id,
    s.scheme_id,
    s.name scheme_name,
    sbf.mov_id,
    sbf.mov_name,
    sbf.hard_mov,
    sbf.max_top_up,
  from mov_components_with_sbf sbf
  left join `fulfillment-dwh-production.dl.dynamic_pricing_price_scheme` s on
    sbf.global_entity_id = s.global_entity_id and sbf.mov_id = s.mov_configuration_id
  where not deleted
  order by 1,2,3
)
select
  asa.entity_id,
  asa_id,
  asa_name,
  asa.assigned_vendors_count,
  pc.is_default_scheme,
  s.* except(global_entity_id),
from `fulfillment-dwh-production.cl.pricing_asa_full_configuration_versions` asa
left join unnest(asa_price_config) pc
inner join schemes_with_sbf s on asa.entity_id = s.global_entity_id and pc.scheme_id = s.scheme_id
where active_to is null
order by 1,4 desc
