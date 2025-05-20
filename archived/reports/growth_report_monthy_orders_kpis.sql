-- drop table pricing.growth_report_monthy_orders_kpis
create or replace table `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis`
partition by month
options(
  partition_expiration_days = null,
  require_partition_filter = false
)
as 
-- (
-- select * from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis`
-- where
--   month < date_sub(date_trunc(current_date(), month), interval 1 month)
-- union all
-- select * 
-- from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis_view`
-- where
--   month >= date_sub(date_trunc(current_date(), month), interval 1 month)
-- )
(
with
entities as (
    select distinct global_entity_id
    from `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities` e
    where e.is_reporting_enabled
        and e.is_platform_online
)
-- , order_service_fee as (
--     select 
--         global_entity_id,
--         order_id,
--         customer.payment.service_fee sf,
--     from `fulfillment-dwh-production.curated_data_shared_data_stream.orders` as orders 
--     inner join entities using (global_entity_id)
--     where created_date between '2018-01-01' and current_date()
-- )
-- , delivery_costs as (
--     select
--         p.entity_id as global_entity_id,
--         p.platform_order_code as order_id,
--         sum(p.costs) delivery_costs,
--     from `fulfillment-dwh-production.curated_data_shared.utr_timings` p
--     where p.created_date between '2018-01-01' and current_date()
--     group by 1,2)
select
    o.global_entity_id,
    v.vertical_parent,
    v.vertical_type,
    o.value.delivery_fee_local = 0 is_free_delivery,
    o.is_own_delivery,
    date(date_trunc(o.placed_at_local, month)) month,
    avg(o.fx_rate_eur) fx_rate_eur,
    sum(o.value.delivery_fee_local) delivery_fee_local,
    --sum(o.value.delivery_fee_eur) delivery_fee_eur,    
    sum(o.value.commission_base_local) commission_base_local,
    --sum(o.value.commission_base_eur) commission_base_eur,
    sum(o.value.commission_local) commission_local,
    --sum(o.value.commission_eur) commission_eur,
    sum(o.value.mov_customer_fee_local) mov_fee_local,
    --sum(o.value.mov_customer_fee_eur) mov_fee_eur,
    sum(o.value.gbv_local) gbv_local,
    --sum(o.value.gbv_eur) gbv_eur,
    sum(o.value.mov_local) mov_local,
    --sum(o.value.mov_eur) mov_eur,
    -- sum(o.value.service_fee_local) service_fee_local,
    sum(sf.service_fee) service_fee_local,
    sum(o.value.voucher_dh_local) voucher_dh_local,
    sum(o.value.discount_dh_local) discount_dh_local,
    sum(o.value.joker_vendor_fee_local) joker_vendor_fee_local,
    sum(c.delivery_costs) delivery_costs,
    count(*) orders,
    sum(case when o.value.delivery_fee_local = 0 then 1 else 0 end) orders_with_free_delivery,
    sum(case when o.value.mov_local > 0 then 1 else 0 end) orders_with_mov,
    sum(case when o.value.mov_customer_fee_local > 0 then 1 else 0 end) orders_with_mov_fee,
    -- sum(case when o.value.service_fee_local > 0 then 1 else 0 end) orders_with_service_fee,
    sum(case when sf.service_fee > 0 then 1 else 0 end) orders_with_service_fee,
    sum(case when o.value.voucher_dh_local > 0 then 1 else 0 end) orders_with_dh_voucher,
    sum(case when o.value.discount_dh_local > 0 then 1 else 0 end) orders_with_dh_discount,
    sum(case when o.value.joker_vendor_fee_local > 0 then 1 else 0 end) orders_with_joker,
from `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` o
inner join entities e using (global_entity_id)
left join `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors` v using (global_entity_id, vendor_id)
left join `dh-logistics-product-ops.pricing.order_service_fee` sf using (global_entity_id, order_id)
left join `dh-logistics-product-ops.pricing.delivery_costs_per_order` c using (global_entity_id, order_id)
where o.is_sent
    and date(o.placed_at_local) between '2015-01-01' and current_date()
group by 1,2,3,4,5,6
)
