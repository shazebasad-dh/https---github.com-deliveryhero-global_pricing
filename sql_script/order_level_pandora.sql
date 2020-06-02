-- Date:            2020/06/02
-- Contributor:     Laurent Broering
-- Enviroment:      Pandora
-- BI consultant:
-- Comment:


select
c.name city_name,
z.name zone_name,
lo.created_date,
o.order_code_google,
lv.vendor_name,
a.porygon_vehicle_profile,
a.drive_time_value,
o.gmv_local,
o.gfv_local,
o.service_fee,
o.delivery_fee,
case when v.type = 'delivery_fee' and o.voucher_used then coalesce(o.voucher_value_local,0) else 0 end as df_voucher,
case when v.type <> 'delivery_fee' and o.voucher_used then coalesce(o.voucher_value_local,0) else 0 end as other_voucher,
case when d.discount_type = 'free-delivery' and o.discount_used then coalesce(o.discount_value_local,0) else 0 end as df_discount,
case when d.discount_type <> 'free-delivery' and o.discount_used then coalesce(o.discount_value_local,0) else 0 end as other_discount
from il_country_hk.v_clg_orders lo
left join il_country_hk.v_clg_order_porygon_attributes a on lo.order_id = a.order_id
left join il_country_hk.v_clg_cities c on lo.city_id = c.city_id
left join il_country_hk.v_clg_zones z on lo.zone_id = z.zone_id
left join il_country_hk.v_clg_vendors lv on lo.vendor_id = lv.vendor_id
left join il_country_hk.v_fct_orders o on o.order_code_google = lo.platform_order_code
left join il_country_hk.v_meta_order_status s on s.status_id = o.status_id
left join il_country_hk.v_dim_vouchers v ON v.voucher_id = o.voucher_id
left join il_country_hk.v_dim_discounts d ON d.discount_id = o.discount_id
where
o.order_date between '2020-04-10' and '2020-04-30'
and s.valid_order = 1
and o.own_delivery
and a.porygon_vehicle_profile = "default"
