drop table if exists peya_orders;
create temp table peya_orders as
    select
        c.short_name,
        o.shipping_amount,
        o.shipping_amount_no_discount,
        o.id
    from dwh_mysql_py.orders as o
    left join dwh_mysql_py.restaurant r on r.id = o.restaurant_id
    left join dwh_mysql_py.country c on c.id = r.country_id
    where o.state = 'CONFIRMED' and o.registered_date::date >= current_date - 107;

drop table if exists log_orders;
create temp table log_orders
distkey(platform_order_code) as
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
        and ((la.country_code in ('bo','pa') and la.porygon_vehicle_profile = 'scooter_v1')
    	or (la.country_code in ('py') and la.porygon_vehicle_profile = 'scooter')
        or (la.country_code not in ('bo','pa') and la.porygon_vehicle_profile = 'bicycle_v2'))
    where
        lo.order_status = 'completed'
        and lo.country_code in ('ar','bo','cl','do','pa','py','uy')
        and lo.order_placed_at between current_date - 107 and current_date;

drop table if exists od_orders;
create temp table od_orders --> usually we don't materialize this but it is used 5 times so for simplicity and preparing diskey we do
distkey(order_id) as --> there is no distkey that seems to win over another one (ie : group bys are done on different dimension on low cardinality values) so we go for optimizing the join with deliveries
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
        o.source_id in (9, 134, 13, 151, 18, 129, 19, 22) --> Argentina, Bolivia, Chile, Dominican Republic, Panama, Panama (Appetito24), Paraguay, Uruguay
        and o.is_dh_delivery --> that should cover logitistic data, if not tickets need to be raised to fix the flag
        and o.order_date between current_date - 107 and current_date;

drop table if exists orders;
create temp table orders
distkey("date") as
    select
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
    group by 1,2,3,4,5,6,7,8,9,10,11,12,df_eur;

drop table if exists deliveries;
create temp table deliveries
distkey(delivery_date) as
    select
        de.rdbms_id,
        de.entity_display_name,
        de.city_id,
        o.zone_id,
        o.is_restaurant,
        de.rider_dropped_off_at::date delivery_date,
        o.drive_time_value::varchar(4)                               as drivingtimebucket,
        sum(de.to_customer_time)                                     as totaldrivingtime,
        count(de.to_customer_time)                                   as totaldrivingswithtime,
        sum(de.dropoff_distance_manhattan)                           as totaldrivingdistance,
        count(*)                                                     as deliveries
    from dwh_redshift_logistic.v_clg_deliveries de
    left join od_orders o on de.rdbms_id = o.rdbms_id and de.entity_display_name = o.entity_display_name and de.order_id = o.order_id
    where de.delivery_status = 'completed'
        and de.rider_dropped_off_at between current_date - 187 and current_date
    group by 1,2,3,4,5,6,7;

drop table if exists shifts;
create temp table shifts
distkey(shift_date)
as
    select
        s.rdbms_id,
        s.city_id,
        s.zone_id,
        s.created_date as shift_date,
        sum(s.actual_working_time) as actualworkingtimeinsec
    from dwh_redshift_logistic.v_clg_shifts s
    where s.created_date between current_date - 107 and current_date
    group by 1,2,3,4;

drop table if exists distinct_data;
create temp table distinct_data
distkey(order_date)
as
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.order_date,
        o.is_restaurant,
        count(distinct(analytical_customer_id)) as distinct_customers,
        count(distinct(restaurant_id)) as distinct_restaurants
    from od_orders o
    group by 1,2,3,4,5,6;

drop table if exists weekly_frequency;
create temp table weekly_frequency
distkey(iso_date)
as
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        d.iso_date,
        o.is_restaurant,
        count(distinct o.analytical_customer_id) as week_valid_customers,
        sum(o.order_qty) as week_valid_orders
    from od_orders o
    inner join dwh_il.dim_date d on o.order_date > d.iso_date - 7
        and o.order_date <= d.iso_date
        and d.iso_date between current_date - 107 and current_date + 7
    group by 1,2,3,4,5,6;

drop table if exists city_id_dictionary;
create temp table city_id_dictionary
diststyle all
as
    select
        source_id,
        backend_city_id,
        hurrier_city_id
    from (select
            o.source_id,
            o.backend_city_id,
            o.city_id as hurrier_city_id,
            row_number() over (partition by o.source_id, o.backend_city_id order by count desc) as rank,
            count(*)
            from od_orders o
            group by 1,2,3)
    where rank = 1;

drop table if exists active_restaurants;
create temp table active_restaurants
distkey("date")
as
    select
        rest.source_id,
        rest.shop_type = 'restaurants' is_restaurant,
        city.hurrier_city_id as city_id,
        hist.valid_at as date,
        count(distinct case when hist.is_online and hist.is_dh_delivery then hist.restaurant_id end) as number_of_od_restaurants,
        count(distinct case when hist.is_online then hist.restaurant_id end) as number_of_restaurants
    from dwh_il.dim_restaurant_history hist
    join dwh_il.dim_restaurant as rest on rest.source_id = hist.source_id and rest.restaurant_id = hist.restaurant_id
    join city_id_dictionary city on rest.city_id = city.backend_city_id and rest.source_id = city.source_id
    where rest.source_id > 0 and hist.is_online and hist.valid_at between current_date - 107 and current_date
    group by 1,2,3,4;

drop table if exists peya_pricing_report;
create temp table peya_pricing_report as
    select
        co.management_entity_group,
        co.company_name,
        co.common_name as country,
        co.country_iso,
        co.currency_code as currency,
        co.region,
        o.entity_display_name,
        o.source_id,
        o.rdbms_id,
        lc.name as city,
        o.city_id,
        z.name as zone,
        o.zone_id,
        o.is_restaurant,
        o.date,
        o.drive_time_value,
        o.py_df_lc,
        o.py_df_lc_discounted,
        o.df_lc,
        o.log_df_lc,
        o.paid_lc,
        o.cv_lc,
        o.voucher_dh_lc,
        o.voucher_other_lc,
        o.discount_dh_lc,
        o.discount_other_lc,
        o.revenue_dh_lc,
        o.commission_lc,
        o.joker_lc,
        o.delivery_fee_lc,
        o.log_delivery_fee_lc,
        o.df_eur,
        o.paid_eur,
        o.cv_eur,
        o.voucher_dh_eur,
        o.voucher_other_eur,
        o.discount_dh_eur,
        o.discount_other_eur,
        o.revenue_dh_eur,
        o.commission_eur,
        o.joker_eur,
        o.delivery_fee_eur,
        o.newcustomers,
        o.orders,
        d.drivingtimebucket,
        d.totaldrivingtime,
        d.totaldrivingswithtime,
        d.totaldrivingdistance,
        d.deliveries,
        s.actualworkingtimeinsec,
        dd.distinct_customers,
        dd.distinct_restaurants,
        w.week_valid_customers,
        w.week_valid_orders,
        r.number_of_od_restaurants,
        r.number_of_restaurants
    from orders o
    left join deliveries d on o.rdbms_id = d.rdbms_id and o.entity_display_name = d.entity_display_name and o.city_id = d.city_id and o.zone_id = d.zone_id and o.date = d.delivery_date and o.drive_time_value = d.drivingtimebucket::int and o.is_restaurant = d.is_restaurant
    left join shifts s on o.rdbms_id = s.rdbms_id and o.city_id = s.city_id and o.zone_id = s.zone_id and o.date = s.shift_date
    left join distinct_data dd on o.rdbms_id = dd.rdbms_id and o.entity_display_name = dd.entity_display_name and o.city_id = dd.city_id and o.zone_id = dd.zone_id and o.date = dd.order_date and o.is_restaurant = dd.is_restaurant
    left join weekly_frequency w on o.rdbms_id = w.rdbms_id and o.entity_display_name = w.entity_display_name and o.city_id = w.city_id and o.zone_id = w.zone_id and o.date = w.iso_date and o.is_restaurant = w.is_restaurant
    left join active_restaurants r on o.source_id = r.source_id and o.city_id = r.city_id and o.date = r.date and o.is_restaurant = r.is_restaurant
    left join dwh_il.dim_countries co on o.source_id = co.source_id
    left join dwh_redshift_logistic.v_clg_cities lc on o.rdbms_id = lc.rdbms_id and o.city_id = lc.city_id
    left join dwh_redshift_logistic.v_clg_zones z on o.rdbms_id = z.rdbms_id and o.city_id = z.city_id and o.zone_id = z.zone_id
    where co.is_active and o.date between current_date - 100 and current_date;