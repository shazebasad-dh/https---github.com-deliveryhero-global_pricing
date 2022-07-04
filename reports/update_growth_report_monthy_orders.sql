select * from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis`
where
  month < date_sub(date_trunc(current_date(), month), interval 1 month)

union all

select * 
from `dh-logistics-product-ops.pricing.growth_report_monthy_orders_kpis_view`
where
  month >= date_sub(date_trunc(current_date(), month), interval 1 month)
