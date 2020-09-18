SELECT  
EXTRACT(WEEK FROM date_local) AS week
, c.id as city_id
, c.name as city_name
, z.id as zone_id
, z.name as zone_name
, is_first_valid_order new_user
, voucher_type
, voucher_value_local
, discount_type
, discount_value_local
, delivery_fee_original_local
, delivery_fee_local
,count(distinct order_code_google)
FROM `dhh---analytics-apac.pandata.fct_orders` pa
right join fulfillment-dwh-production.cl.orders o on o.platform_order_code = pa.order_code_google and lower(o.country_code) = lower(pa.country_iso_code)
left join unnest(deliveries) d
left join unnest(porygon) p
left join cl.countries on o.country_code = countries.country_code
left join unnest(cities) c on c.id = o.city_id
left join unnest(zones) z on z.id = o.zone_id
WHERE created_date_local > "2020-05-01" 
and o.entity.id = "FP_TW"
and rdbms_id = 18
and is_valid_order
and o.order_status = 'completed'
and d.delivery_status = 'completed'
and p.vehicle_profile = 'default'
group by 1,2,3,4,5,6,7,8,9,10,11,12
