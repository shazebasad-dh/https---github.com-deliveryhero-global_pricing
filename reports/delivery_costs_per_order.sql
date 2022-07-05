create or replace table `dh-logistics-product-ops.pricing.delivery_costs_per_order`
partition by created_date
options(
  partition_expiration_days=null,
  require_partition_filter=false
)
as 
(
select
    p.entity_id as global_entity_id,
    p.created_date,
    p.platform_order_code as order_id,
    sum(p.costs) delivery_costs,
from `fulfillment-dwh-production.curated_data_shared.utr_timings` p
where p.created_date between '2018-01-01' and current_date()
group by 1,2,3
)
