-- growth_report_monthy_orders_kpis

create or replace table `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis`
partition by month
options(
  partition_expiration_days=null,
  require_partition_filter=false
)
as 
(
select * from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis`
where
  month < date_sub(date_trunc(current_date(), month), interval 1 month)
union all
select * 
from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis_view`
where
  month >= date_sub(date_trunc(current_date(), month), interval 1 month)
);

-- order_service_fee

create or replace table `dh-logistics-product-ops.pricing.order_service_fee`
partition by created_date
options(
  partition_expiration_days=null,
  require_partition_filter=false
)
as 
(
select * from `dh-logistics-product-ops.pricing.order_service_fee`
where
  created_date < date_sub(current_date(), interval 7 day)
union all
select * 
from `dh-logistics-product-ops.pricing.order_service_fee_view`
where
  created_date >= date_sub(current_date(), interval 7 day)
);

-- delivery_costs_per_order

create or replace table `dh-logistics-product-ops.pricing.delivery_costs_per_order`
partition by created_date
options(
  partition_expiration_days=null,
  require_partition_filter=false
)
as 
(
select * from `dh-logistics-product-ops.pricing.delivery_costs_per_order`
where
  created_date < date_sub(date_trunc(current_date(), day), interval 15 day)
union all
select * 
from `dh-logistics-product-ops.pricing.delivery_costs_per_order_view`
where
  created_date >= date_sub(date_trunc(current_date(), day), interval 15 day)
);
