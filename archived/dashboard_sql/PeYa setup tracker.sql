with peya_orders as (
    select
        c.short_name,
        o.shipping_amount,
        o.shipping_amount_no_discount,
        o.id
    from dwh_mysql_py.orders as o
    left join dwh_mysql_py.restaurant r on r.id = o.restaurant_id
    left join dwh_mysql_py.country c on c.id = r.country_id
    where o.state = 'CONFIRMED' and o.registered_date::date >= current_date - 107),

log_orders as (
    select
        --region, company--
        m.source_id,
        --country--
        lo.rdbms_id,
        lo.entity_display_name,
        --city, zone--
        lo.city_id,
        coalesce(lo.zone_id,0) as zone_id, -- Adjustment to the orders with no zone assigned
        --orders data--
        lo.platform_order_code,
        lo.order_id,
        lo.delivery_fee/100 as log_df_lc,
        o.shipping_amount,
        o.shipping_amount_no_discount,
        la.drive_time_value
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_il.dim_countries m on m.dwh_source_code = lo.entity_id
    left join peya_orders o on lo.platform_order_code = o.id AND o.short_name = lo.country_code
    left join dwh_redshift_logistic.v_clg_order_porygon_attributes la on
        lo.order_id = la.order_id and lo.country_code = la.country_code
        and ((la.country_code in ('bo','pa','ve') and la.porygon_vehicle_profile = 'scooter_v1')
        or (la.country_code in ('py') and la.porygon_vehicle_profile = 'scooter')
        or (la.country_code in ('ar','cl','do','uy') and la.porygon_vehicle_profile = 'bicycle_v2'))
    where
        lo.order_status = 'completed'
        and lo.country_code in ('ar','bo','cl','do','pa','py','uy','ve')
        and lo.order_placed_at between current_date - 107 and current_date),

od_orders as (
    select
        --entity--
        o.source_id,
        lo.rdbms_id,
        lo.entity_display_name,
        --city--
        r.city_id as backend_city_id,
        lo.city_id,
        --zone--
        lo.zone_id as zone_id,
        --orders data--
        lo.platform_order_code,
        lo.order_id,
        o.restaurant_id,
        r.shop_type = 'restaurants' is_restaurant,
        o.analytical_customer_id,
        o.order_date::date,
        o.is_acquisition,
        o.order_qty,
        lo.drive_time_value,
        --hurrier df--
        lo.log_df_lc,
        --peya df--
        lo.shipping_amount,
        lo.shipping_amount_no_discount,
        --rev in lc--
        o.amt_paid_lc,
        o.amt_cv_lc,
        o.amt_commission_lc,
        o.amt_joker_lc,
        o.amt_delivery_fee_lc,
        o.amt_dh_revenue_lc,
        o.amt_discount_dh_lc,
        o.amt_discount_other_lc,
        o.amt_voucher_dh_lc,
        o.amt_voucher_other_lc,
        --rev in lc--
        o.amt_paid_eur,
        o.amt_cv_eur,
        o.amt_commission_eur,
        o.amt_joker_eur,
        o.amt_delivery_fee_eur,
        o.amt_dh_revenue_eur,
        o.amt_discount_dh_eur,
        o.amt_discount_other_eur,
        o.amt_voucher_dh_eur,
        o.amt_voucher_other_eur
    from dwh_il.ranked_fct_order o
    join log_orders lo on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id and o.source_id = r.source_id
    where
        o.source_id in (9, 134, 13, 151, 18, 129, 19, 22, 4) --> Argentina, Bolivia, Chile, Dominican Republic, Panama, Panama (Appetito24), Paraguay, Uruguay, Venezuela
        and o.is_dh_delivery --> that should cover logitistic data, if not tickets need to be raised to fix the flag
        and o.order_date between current_date - 107 and current_date)

select
    co.management_entity_group,
    co.company_name,
    co.common_name as country,
    co.country_iso,
    co.currency_code as currency,
    co.region,
    lc.name as city,
    z.name as zone,
    o.source_id                                                  as source_id,
    o.rdbms_id                                                   as rdbms_id,
    o.entity_display_name                                        as entity_display_name,
    o.city_id                                                    as city_id,
    o.zone_id                                                    as zone_id,
    o.is_restaurant                                              as is_restaurant,
    o.order_date                                                 as date,
    o.drive_time_value                                           as drive_time_value,
    -- peya delivery fee --
    o.shipping_amount                                            as py_df_lc_discounted,
    o.shipping_amount_no_discount                                as py_df_lc,
    -- delivery fee from hurrier --
    o.log_df_lc                                                  as log_df_lc,
    -- revenue in lc --
    o.amt_delivery_fee_lc                                        as df_lc,
    sum(o.amt_paid_lc)                                           as paid_lc,
    sum(o.amt_cv_lc)                                             as cv_lc,
    sum(o.amt_voucher_dh_lc)                                     as voucher_dh_lc,
    sum(o.amt_voucher_other_lc)                                  as voucher_other_lc,
    sum(o.amt_discount_dh_lc)                                    as discount_dh_lc,
    sum(o.amt_discount_other_lc)                                 as discount_other_lc,
    sum(o.amt_dh_revenue_lc)                                     as revenue_dh_lc,
    sum(o.amt_commission_lc)                                     as commission_lc,
    sum(o.amt_joker_lc)                                          as joker_lc,
    sum(o.amt_delivery_fee_lc)                                   as delivery_fee_lc,
    sum(o.log_df_lc)                                             as log_delivery_fee_lc,
    -- revenue in eur --
    o.amt_delivery_fee_eur                                       as df_eur,
    sum(o.amt_paid_eur)                                          as paid_eur,
    sum(o.amt_cv_eur)                                            as cv_eur,
    sum(o.amt_voucher_dh_eur)                                    as voucher_dh_eur,
    sum(o.amt_voucher_other_eur)                                 as voucher_other_eur,
    sum(o.amt_discount_dh_eur)                                   as discount_dh_eur,
    sum(o.amt_discount_other_eur)                                as discount_other_eur,
    sum(o.amt_dh_revenue_eur)                                    as revenue_dh_eur,
    sum(o.amt_commission_eur)                                    as commission_eur,
    sum(o.amt_joker_eur)                                         as joker_eur,
    sum(o.amt_delivery_fee_eur)                                  as delivery_fee_eur,
    -- volume --
    sum(case when o.is_acquisition then o.order_qty else 0 end)  as newcustomers, -- Number of first *successful* orders // first_order_all considers the first order regardless of its final status
    sum(o.order_qty)                                             as orders
from od_orders o
left join dwh_il.dim_countries co using (source_id)
left join dwh_redshift_logistic.v_clg_cities lc using (rdbms_id, city_id)
left join dwh_redshift_logistic.v_clg_zones z using (rdbms_id, city_id, zone_id)
where co.is_active
group by
    1,2,3,4,5,6,7,8,
    9,10,11,12,13,14,15,16,
    py_df_lc_discounted,py_df_lc,log_df_lc,df_lc,df_eur