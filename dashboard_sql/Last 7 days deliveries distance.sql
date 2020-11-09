drop table if exists order_id_number;
create temp table order_id_number -- To avoid recreating ranked_fct_order we will align logistics data to it instead. Since redshift is columnar it is ok to scan ranked_fct_order for just 3 columns
distkey(order_number) as
    select
        rfo.source_id,
        rfo.order_id,
        rfo.order_number
    from dwh_il.ranked_fct_order rfo
    where rfo.source_id in (6, 39, 97, 143, 32) --> Countries that we should join with log data using order_number instead of order_id: Yogiyo KR, Mjam AT, Netpincer HU, Hungrig SE, Yemeksepeti TR
        and rfo.is_dh_delivery --> Should cover all logistic data, report exceptions with a ticket to DATA
        and rfo.is_sent
        and rfo.order_date between current_date - 7 and current_date;

drop table if exists log_orders;
create temp table log_orders
distkey(platform_order_code) as
    select
        dc.source_id,
        lo.rdbms_id,
        lo.entity_display_name,
        lo.city_id,
        coalesce(lo.zone_id,0) as zone_id, -- Adjustment to the orders with no zone assigned
        -- Orders ids --
        case when lo.platform_order_code like 'CG-%'  -- Carriage only had one backend; order_id is unique
                then split_part(lo.platform_order_code, '-', 3)
             when platform_order_code like 'zo-%' -- We only have one country in Zomato (UAE); order_id is also unique
                then split_part(lo.platform_order_code, '-', 2)
             else coalesce(o.order_id, lo.platform_order_code)
        end as platform_order_code,
        lo.order_id,
        lo.customer_location,
        coalesce(v.last_provided_location, v.location) as vendor_location
/*        lo.delivery_fee/100 as log_df_lc*/
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_redshift_logistic.v_clg_vendors v on lo.country_code = v.country_code and lo.vendor_id = v.hurrier_id
    left join dwh_il.dim_countries dc on dc.dwh_source_code = lo.entity_id
    left join order_id_number o on dc.source_id = o.source_id and lo.platform_order_code = o.order_number
    where lo.order_status = 'completed'
        and lo.order_placed_at between current_date - 7 and current_date;

drop table if exists od_orders;
create temp table od_orders --> usually we don't materialize this but it is used 5 times so for simplicity and preparing diskey we do
distkey(order_id) as --> there is no distkey that seems to win over another one (ie : group bys are done on different dimension on low cardinality values) so we go for optimizing the join with deliveries
    select
        o.source_id,
        lo.rdbms_id,
        lo.entity_display_name,
        r.city_id as backend_city_id,
        lo.city_id,
        lo.zone_id as zone_id,
/*        lo.platform_order_code,*/
        lo.order_id,
        lo.customer_location,
        lo.vendor_location,
/*        o.restaurant_id,*/
        r.shop_type,
/*        o.analytical_customer_id,
        o.is_acquisition,*/
        o.order_date::date,
/*        -- Hurrier DF --
        lo.log_df_lc,
        -- Rev in LC --
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
        -- Rev in EUR --
        o.amt_paid_eur,
        o.amt_cv_eur,
        o.amt_commission_eur,
        o.amt_joker_eur,
        o.amt_delivery_fee_eur,
        o.amt_dh_revenue_eur,
        o.amt_discount_dh_eur,
        o.amt_discount_other_eur,
        o.amt_voucher_dh_eur,
        o.amt_voucher_other_eur,
        -- Volume --
        o.order_qty*/
    from log_orders lo
    left join dwh_il.ranked_fct_order o on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id and o.source_id = r.source_id
    where o.is_dh_delivery --> should cover all logistic data, report exceptions in a ticket under DATA
        and o.is_sent --> should filter out around 0.25% of all completed orders
        and o.order_date between current_date - 7 and current_date;

select
    de.rdbms_id                                                  as rdbms_id,
    de.entity_display_name                                       as entity_display_name,
    de.city_id                                                   as city_id,
    o.zone_id                                                    as zone_id,
    o.shop_type                                                  as shop_type,
    de.rider_dropped_off_at::date                                as date,
    de.delivery_id                                               as delivery_id,
    (de.dropoff_distance_manhattan)                              as dropoff_distance_manhattan,
    /*(de.dropoff_distance_manhattan)                              as totaldrivingswithdistance,*/
    ST_DistanceSphere(
        ST_GeomFromText(de.pickup),
        ST_MakePoint(
            ST_X(ST_GeomFromText(de.pickup)),
            ST_Y(ST_GeomFromText(de.dropoff)))) +
    ST_DistanceSphere(
        ST_GeomFromText(de.dropoff),
        ST_MakePoint(
            ST_X(ST_GeomFromText(de.pickup)),
            ST_Y(ST_GeomFromText(de.dropoff))))                  as manual_manhattan,
    ST_DistanceSphere(
        ST_GeomFromText(de.dropoff),
        ST_GeomFromText(o.customer_location))                    as dropoff_2_customer,
    ST_DistanceSphere(
        ST_GeomFromText(de.pickup),
        ST_GeomFromText(o.vendor_location))                      as pickup_2_vendor,
    1                                                            as deliveries
from od_orders o
left join dwh_redshift_logistic.v_clg_deliveries de using (rdbms_id, entity_display_name, order_id)
where de.delivery_status = 'completed'
    and de.rider_dropped_off_at between current_date - 7 and current_date