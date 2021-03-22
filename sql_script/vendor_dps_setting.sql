  
-- Date:            2021/03/22
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   
-- Comment:  


SELECT 
vendor_code
, d.is_active
, d.variant
, d.scheme_id
, d.is_scheme_fallback
, string_agg(distinct cast (p.travel_time_config.threshold as string),"," order by cast (p.travel_time_config.threshold as string)) travel_time_threshold
, string_agg(distinct cast (p.travel_time_config.fee as string),"," order by cast (p.travel_time_config.fee as string)) travel_time_fee
, string_agg(distinct cast (p.mov_config.travel_time_threshold as string),"," order by cast (p.mov_config.travel_time_threshold as string)) mov_threshold
, string_agg(distinct cast (p.mov_config.minimum_order_value as string),"," order by cast (p.mov_config.minimum_order_value as string)) mov_fee
, string_agg(distinct cast (p.delay_config.delay_threshold as string),"," order by cast (p.delay_config.delay_threshold as string)) delay_threshold
, string_agg(distinct cast (p.delay_config.travel_time_threshold as string),"," order by cast (p.delay_config.travel_time_threshold as string)) delay_travel_time_threshold
, string_agg(distinct cast (p.delay_config.fee as string),"," order by cast (p.delay_config.fee as string)) surge_fee
FROM `fulfillment-dwh-production.cl.vendors_v2` v, v.dps d, d.vendor_config co, co.pricing_config p
where entity_id = "FP_HK"
and vertical_type = "restaurants"
and "OWN_DELIVERY" in unnest(v.delivery_provider)
group by 1,2,3,4,5
