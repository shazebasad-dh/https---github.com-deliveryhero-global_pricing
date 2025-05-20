drop table dwh_bl.tableau_pricing_report;
create table dwh_bl.tableau_pricing_report (
  management_entity_group         VARCHAR(30)
  ,company_name                   VARCHAR(30)
  ,country                        VARCHAR(30)
  ,country_iso                    VARCHAR(4)
  ,currency                       VARCHAR(3)
  ,region                         VARCHAR(10)
  ,entity_display_name            VARCHAR(50)
  ,source_id                      SMALLINT
  ,rdbms_id                       SMALLINT
  ,city                           VARCHAR(50)
  ,city_id                        SMALLINT
  ,zone                           VARCHAR(50)
  ,zone_id                        SMALLINT
  ,is_restaurant                  BOOLEAN
  ,date                           DATE
  -- Delivery fee from Hurrier --
  ,log_df_lc                      NUMERIC(36,2)
  ,log_delivery_fee_lc            NUMERIC(36,2)
  -- Revenue in LC --
  ,df_lc                          NUMERIC(19,2)
  ,paid_lc                        NUMERIC(36,2)
  ,cv_lc                          NUMERIC(36,2)
  ,voucher_dh_lc                  NUMERIC(36,2)
  ,voucher_other_lc               NUMERIC(36,2)
  ,discount_dh_lc                 NUMERIC(36,2)
  ,discount_other_lc              NUMERIC(36,2)
  ,revenue_dh_lc                  NUMERIC(36,2)
  ,commission_lc                  NUMERIC(36,2)
  ,joker_lc                       NUMERIC(36,2)
  ,delivery_fee_lc                NUMERIC(36,2)
  -- Revenue in EUR --
  ,df_eur                         NUMERIC(12,2)
  ,paid_eur                       NUMERIC(12,2)
  ,cv_eur                         NUMERIC(12,2)
  ,voucher_dh_eur                 NUMERIC(12,2)
  ,voucher_other_eur              NUMERIC(12,2)
  ,discount_dh_eur                NUMERIC(12,2)
  ,discount_other_eur             NUMERIC(12,2)
  ,revenue_dh_eur                 NUMERIC(12,2)
  ,commission_eur                 NUMERIC(12,2)
  ,joker_eur                      NUMERIC(12,2)
  ,delivery_fee_eur               NUMERIC(12,2)
  -- Volume --
  ,newcustomers                   INTEGER
  ,orders                         INTEGER
  -- Delivery related data --
  ,drivingtimebucket              VARCHAR(5)
  ,totaldrivingtime               INTEGER
  ,totaldrivingswithtime          INTEGER
  ,totaldrivingdistance           INTEGER
  ,totaldrivingswithdistance      INTEGER
  ,deliveries                     INTEGER
  -- Shfits --
  ,actualworkingtimeinsec         INTEGER
  -- Daily data --
  ,distinct_customers             INTEGER
  ,distinct_vendors               INTEGER
  -- Weekly data --
  ,week_valid_customers           INTEGER
  ,week_valid_orders              INTEGER
  -- Online vendors --
  ,number_of_od_vendors           INTEGER
  ,number_of_vendors              INTEGER
) DISTSTYLE EVEN;

--drop table if exists order_id_number;
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
        and rfo.order_date between current_date - 187 and current_date;

--drop table if exists log_orders;
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
        lo.delivery_fee/100 as log_df_lc
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_il.dim_countries dc on dc.dwh_source_code = lo.entity_id
    left join order_id_number o on dc.source_id = o.source_id and lo.platform_order_code = o.order_number
    where lo.order_status = 'completed'
        and lo.order_placed_at between current_date - 187 and current_date;

--drop table if exists od_orders;
create temp table od_orders --> usually we don't materialize this but it is used 5 times so for simplicity and preparing diskey we do
distkey(order_id) as --> there is no distkey that seems to win over another one (ie : group bys are done on different dimension on low cardinality values) so we go for optimizing the join with deliveries
    select
        o.source_id,
        lo.rdbms_id,
        lo.entity_display_name,
        r.city_id as backend_city_id,
        lo.city_id,
        lo.zone_id as zone_id,
        lo.platform_order_code,
        lo.order_id,
        o.restaurant_id,
        r.shop_type = 'restaurants' is_restaurant,
        o.analytical_customer_id,
        o.is_acquisition,
        o.order_date::date,
        -- Hurrier DF --
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
        o.order_qty
    from log_orders lo
    left join dwh_il.ranked_fct_order o on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id and o.source_id = r.source_id
    where o.is_dh_delivery --> should cover all logistic data, report exceptions in a ticket under DATA
        and o.is_sent --> should filter out around 0.25% of all completed orders
        and o.order_date between current_date - 187 and current_date;

--drop table if exists orders;
create temp table orders
distkey(date) as
    select
        o.source_id                                                  as source_id,
        o.rdbms_id                                                   as rdbms_id,
        o.entity_display_name                                        as entity_display_name,
        o.city_id                                                    as city_id,
        o.zone_id                                                    as zone_id,
        o.is_restaurant                                              as is_restaurant,
        o.order_date                                                 as date,
        -- Delivery fee from Hurrier --
        o.log_df_lc                                                  as log_df_lc,
        sum(o.log_df_lc)                                             as log_delivery_fee_lc,
        -- Revenue in LC --
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
        -- Revenue in EUR --
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
        -- Volume --
        sum(case when o.is_acquisition then o.order_qty else 0 end)  as newcustomers, -- Number of first *successful* orders // first_order_all considers the first order regardless of its final status
        sum(o.order_qty)                                             as orders
    from od_orders o
    group by 1,2,3,4,5,6,7,8,10,21;

--drop table if exists deliveries;
create temp table deliveries
distkey(date) as
    select
        de.rdbms_id                                                  as rdbms_id,
        de.entity_display_name                                       as entity_display_name,
        de.city_id                                                   as city_id,
        o.zone_id                                                    as zone_id,
        o.is_restaurant                                              as is_restaurant,
        de.rider_dropped_off_at::date                                as date,
        case
            when de.to_customer_time < 5*60.0 then '<05'
            when de.to_customer_time < 10*60.0 then '<10'
            when de.to_customer_time < 15*60.0 then '<15'
            when de.to_customer_time < 20*60.0 then '<20'
            when de.to_customer_time >= 20*60.0 then '>=20'
            else 'N/A' end                                           as drivingtimebucket,
        sum(de.to_customer_time)                                     as totaldrivingtime,
        count(de.to_customer_time)                                   as totaldrivingswithtime,
        sum(de.dropoff_distance_manhattan)                           as totaldrivingdistance,
        count(de.dropoff_distance_manhattan)                         as totaldrivingswithdistance,
        count(*)                                                     as deliveries
    from od_orders o
    left join dwh_redshift_logistic.v_clg_deliveries de using (rdbms_id, entity_display_name, order_id)
    where de.delivery_status = 'completed'
        and de.rider_dropped_off_at between current_date - 187 and current_date
    group by 1,2,3,4,5,6,7;

--drop table if exists shifts;
create temp table shifts
distkey(date)
as
    select
        s.rdbms_id,
        s.city_id,
        s.zone_id,
        s.created_date as date,
        sum(s.actual_working_time) as actualworkingtimeinsec
    from dwh_redshift_logistic.v_clg_shifts s
    where s.created_date between current_date - 187 and current_date
    group by 1,2,3,4;

--drop table if exists distinct_data;
create temp table distinct_data
distkey(date)
as
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.is_restaurant,
        o.order_date as date,
        count(distinct(analytical_customer_id)) as distinct_customers,
        count(distinct(restaurant_id)) as distinct_vendors
    from od_orders o
    group by 1,2,3,4,5,6;

--drop table if exists weekly_frequency;
create temp table weekly_frequency
distkey(date)
as
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.is_restaurant,
        d.iso_date as date,
        count(distinct o.analytical_customer_id) as week_valid_customers,
        sum(o.order_qty) as week_valid_orders
    from od_orders o
    inner join dwh_il.dim_date d on o.order_date > d.iso_date - 7
        and o.order_date <= d.iso_date
        and d.iso_date between current_date - 187 and current_date + 7
    group by 1,2,3,4,5,6;

--drop table if exists city_id_dictionary;
create temp table city_id_dictionary
diststyle all
as
    select
        source_id,
        city_id,
        hurrier_city_id
    from (select
            o.source_id,
            o.backend_city_id as city_id,
            o.city_id as hurrier_city_id,
            row_number() over (partition by o.source_id, o.backend_city_id order by count desc) as rank,
            count(*)
            from od_orders o
            group by 1,2,3)
    where rank = 1;

--drop table if exists active_restaurants;
create temp table active_vendors
distkey(date)
as
    select
        hist.source_id,
        dict.hurrier_city_id as city_id,
        rest.shop_type = 'restaurants' is_restaurant,
        hist.valid_at as date,
        count(distinct case when hist.is_online and hist.is_dh_delivery then hist.restaurant_id end) as number_of_od_vendors,
        count(distinct case when hist.is_online then hist.restaurant_id end) as number_of_vendors
    from dwh_il.dim_restaurant_history hist
    left join dwh_il.dim_restaurant as rest using (source_id, restaurant_id)
    left join city_id_dictionary dict using (source_id, city_id)
    where rest.source_id > 0
        and hist.is_online
        and hist.valid_at between current_date - 187 and current_date
    group by 1,2,3,4;

truncate table dwh_bl.tableau_pricing_report;
insert into dwh_bl.tableau_pricing_report
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
        -- Delivery fee from Hurrier --
        o.log_df_lc,
        o.log_delivery_fee_lc,
        -- Revenue in LC --
        o.df_lc,
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
        -- Revenue in EUR --
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
        -- Volume --
        o.newcustomers,
        o.orders,
        -- Delivery related data --
        d.drivingtimebucket,
        d.totaldrivingtime,
        d.totaldrivingswithtime,
        d.totaldrivingdistance,
        d.totaldrivingswithdistance,
        d.deliveries,
        -- Shfits --
        s.actualworkingtimeinsec,
        -- Daily data --
        dd.distinct_customers,
        dd.distinct_vendors,
        -- Weekly data --
        w.week_valid_customers,
        w.week_valid_orders,
        -- Online vendors --
        r.number_of_od_vendors,
        r.number_of_vendors
    from orders o
    left join deliveries d using (rdbms_id, entity_display_name, city_id, zone_id, is_restaurant, date)
    left join shifts s using (rdbms_id, city_id, zone_id, date)
    left join distinct_data dd using (rdbms_id, entity_display_name, city_id, zone_id, is_restaurant, date)
    left join weekly_frequency w using (rdbms_id, entity_display_name, city_id, zone_id, is_restaurant, date)
    left join active_vendors r using (source_id, city_id, is_restaurant, date)
    left join dwh_il.dim_countries co using (source_id)
    left join dwh_redshift_logistic.v_clg_cities lc using (rdbms_id, city_id)
    left join dwh_redshift_logistic.v_clg_zones z using (rdbms_id, city_id, zone_id)
    where co.is_active
        and o.date between current_date - 180 and current_date;

analyze dwh_bl.tableau_pricing_report predicate columns;