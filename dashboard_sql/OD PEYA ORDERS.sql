DROP TABLE IF EXISTS bi_global_pricing_dev.peya_orders;
CREATE table bi_global_pricing_dev.peya_orders as (
    select
        c.short_name,
        o.shipping_amount,
        o.shipping_amount_no_discount,
        p.name payment_method,
        o.total_amount,
        o.commission,
        o.commission_includes_delivery_cost,
        o.id
    from dwh_mysql_py.orders as o
    left join dwh_mysql_py.restaurant r on r.id = o.restaurant_id
    left join dwh_mysql_py.country c on c.id = r.country_id
    LEFT JOIN dwh_mysql_py.payment_method p ON o.payment_method_id = p.id
    where o.state = 'CONFIRMED' and o.registered_date::date >= current_date - 187);

DROP TABLE IF EXISTS bi_global_pricing_dev.log_peya_orders;
CREATE TABLE bi_global_pricing_dev.log_orders as (
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
        o.payment_method,
        o.total_amount,
        o.commission,
        o.commission_includes_delivery_cost,
        la.drive_time_value
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_il.dim_countries m on m.dwh_source_code = lo.entity_id
    left join bi_global_pricing_dev.peya_orders o on lo.platform_order_code = o.id AND o.short_name = lo.country_code
    left join dwh_redshift_logistic.v_clg_order_porygon_attributes la on
        lo.order_id = la.order_id and lo.country_code = la.country_code
        and ((la.country_code in ('bo','pa') and la.porygon_vehicle_profile = 'scooter_v1')
        or (la.country_code in ('py') and la.porygon_vehicle_profile = 'scooter')
        or (la.country_code not in ('bo','pa') and la.porygon_vehicle_profile = 'bicycle_v2'))
    where
        lo.order_status = 'completed'
        and lo.country_code in ('ar','bo','cl','do','pa','py','uy')
        and lo.order_placed_at between current_date - 187 and current_date);

DROP TABLE IF EXISTS bi_global_pricing_dev.od_peya_orders;
CREATE TABLE bi_global_pricing_dev.od_orders as (
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
        lo.payment_method,
        lo.total_amount,
        lo.commission,
        lo.commission_includes_delivery_cost,
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
    join bi_global_pricing_dev.log_orders lo on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id and o.source_id = r.source_id
    where
        o.source_id in (9, 134, 13, 151, 18, 129, 19, 22) --> Argentina, Bolivia, Chile, Dominican Republic, Panama, Panama (Appetito24), Paraguay, Uruguay
        and o.is_dh_delivery --> that should cover logitistic data, if not tickets need to be raised to fix the flag
        and o.order_date between current_date - 187 and current_date);