create or replace table `dh-logistics-product-ops.pricing.order_service_fee`
partition by created_date
options(
  partition_expiration_days = null,
  require_partition_filter = true
)
as 
with
entities as (
    select distinct global_entity_id
    from `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities` e
    where e.is_reporting_enabled
        and e.is_platform_online
)
select 
  orders.global_entity_id,
  v.vertical_parent,
  v.vertical_type,
  order_id,
  created_date,
  customer.payment.service_fee,
from `fulfillment-dwh-production.curated_data_shared_data_stream.orders` as orders
inner join entities using (global_entity_id)
left join `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v on  orders.global_entity_id = v.global_entity_id and orders.vendor.id = v.vendor_id
where created_date between '2019-01-01' and current_date() -- no relevant data prior to 2019 available
  and customer.payment.service_fee > 0
