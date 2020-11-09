create table dwh_bl.tableau_pricing_report
(
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
  ,date                           DATE
  ,df_lc                          NUMERIC(19,2)
  ,log_df_lc                      NUMERIC(36,2)
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
  ,log_delivery_fee_lc            NUMERIC(36,2)
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
  ,newcustomers                   INTEGER
  ,orders                         INTEGER
  ,drivingtimebucket              VARCHAR(5)
  ,totaldrivingtime               INTEGER
  ,totaldrivingswithtime          INTEGER
  ,totaldrivingdistance           INTEGER
  ,totaldrivingswithdistance      INTEGER
  ,deliveries                     INTEGER
  ,actualworkingtimeinsec         INTEGER
  ,distinct_customers             INTEGER
  ,distinct_restaurants           INTEGER
  ,week_valid_customers           INTEGER
  ,week_valid_orders              INTEGER
  ,number_of_od_restaurants       INTEGER
  ,number_of_restaurants          INTEGER
)
DISTSTYLE EVEN
;

drop table if exists source_id_mapping;
create temp table source_id_mapping
diststyle all as
    select
        dc.source_id,
        case dc.source_id
            when 68 then 40 -- onlinepizza to foodora sweden
            when 18 then 129 -- pedidosya panama to appetito24, but order id does not seem to be matching for those ~600 orders
            else source_id
        end as new_source_id
    from dwh_il.dim_countries dc
    where dc.is_active;

drop table if exists order_id_number;
create temp table order_id_number -- we don't want to recreate ranked_fct_order so we will just align logistics data to it / redshift is columnar it is ok to scan ranked_fct_order for 3 columns
distkey(order_number) as
    select
        rfo.source_id,
        rfo.order_id,
        rfo.order_number
    from dwh_il.ranked_fct_order rfo
    where rfo.source_id in (39, 97, 143, 32)
        and rfo.order_date between current_date - 187 and current_date
        and rfo.is_dh_delivery; --> shoud cover logistic data -- if nto ticket to be raised for dwh

--drop table if exists log_orders;
create temp table log_orders
distkey(platform_order_code) as
    select
        --region, company--
        m.source_id,
        --country--
        case
            when lo.rdbms_id = 88 then 144 else lo.rdbms_id -- 88 (foodora Sweden) was shut down and replaced by 144 (Onlinepizza Sweden) which then was rebranded to Foodora Sweden on 2020-01-09
        end as rdbms_id,
        lo.entity_display_name,
        --city, zone--
        lo.city_id,
        coalesce(lo.zone_id,0) as zone_id, -- Adjustment to the orders with no zone assigned
        --orders data--
        case when lo.platform_order_code like 'CG-%'  -- Carriage only had one backend; order_id is unique
                then split_part(lo.platform_order_code, '-', 3)
             when platform_order_code like 'zo-%' -- We only have one country in Zomato (UAE); order_id is also unique
                then split_part(lo.platform_order_code, '-', 2)
             else coalesce(o.order_id, lo.platform_order_code)
        end as platform_order_code,
        lo.order_id as log_order_id,
        lo.order_placed_at::date as delivery_date,
        lo.delivery_fee/100 as log_df_lc
    from dwh_redshift_logistic.v_clg_orders lo
    left join bi_global_pricing_dev.pricing_mapping_source_rdbms_entity m on m.rdbms_id = lo.rdbms_id and m.entity_display_name = lo.entity_display_name
    left join order_id_number o on m.source_id = o.source_id and lo.platform_order_code = o.order_number
    where lo.order_status = 'completed' and lo.order_placed_at between current_date - 187 and current_date;

--drop table if exists od_orders;
create temp table od_orders --> usually we don't materialize this but it is used 5 times so for simplicity and preparing diskey we do
distkey(log_order_id) as --> there is no distkey that seems to win over another one (ie : group bys are done on different dimension on low cardinality values) so we go for optimizing the join with deliveries
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
        lo.log_order_id,
        o.order_id,
        o.restaurant_id,
        o.analytical_customer_id,
        o.order_date::date as order_date,
        lo.delivery_date,
        o.is_acquisition,
        o.order_qty,
        --hurrier df--
        lo.log_df_lc,
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
    join source_id_mapping sim ON o.source_id = sim.source_id
    join log_orders lo on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id and o.source_id = r.source_id
    where o.order_date between current_date - 187 and current_date
        and o.is_dh_delivery; --> that should cover logitistic data, if not tickets need to be raised to fix the flag

--drop table if exists orders;
create temp table orders
distkey("Date") as
    select
        o.source_id                                                  as Source_Id,
        o.rdbms_id                                                   as Rdbms_Id,
        o.entity_display_name                                        as Entity_Display_Name,
        o.city_id                                                    as City_Id,
        o.zone_id                                                    as Zone_Id,
        o.order_date                                                 as Date,
        -- Delivery fee from Hurrier --
        o.log_df_lc                                                  as Log_DF_LC,
        -- Revenue in LC --
        o.amt_delivery_fee_lc                                        as DF_LC,
        sum(o.amt_paid_lc)                                           as Paid_LC,
        sum(o.amt_cv_lc)                                             as CV_LC,
        sum(o.amt_voucher_dh_lc)                                     as Voucher_DH_LC,
        sum(o.amt_voucher_other_lc)                                  as Voucher_Other_LC,
        sum(o.amt_discount_dh_lc)                                    as Discount_DH_LC,
        sum(o.amt_discount_other_lc)                                 as Discount_Other_LC,
        sum(o.amt_dh_revenue_lc)                                     as Revenue_DH_LC,
        sum(o.amt_commission_lc)                                     as Commission_LC,
        sum(o.amt_joker_lc)                                          as Joker_LC,
        sum(o.amt_delivery_fee_lc)                                   as Delivery_Fee_LC,
        sum(o.log_df_lc)                                             as Log_Delivery_Fee_LC,
        -- Revenue in EUR --
        o.amt_delivery_fee_eur                                       as DF_EUR,
        sum(o.amt_paid_eur)                                          as Paid_EUR,
        sum(o.amt_cv_eur)                                            as CV_EUR,
        sum(o.amt_voucher_dh_eur)                                    as Voucher_DH_EUR,
        sum(o.amt_voucher_other_eur)                                 as Voucher_Other_EUR,
        sum(o.amt_discount_dh_eur)                                   as Discount_DH_EUR,
        sum(o.amt_discount_other_eur)                                as Discount_Other_EUR,
        sum(o.amt_dh_revenue_eur)                                    as Revenue_DH_EUR,
        sum(o.amt_commission_eur)                                    as Commission_EUR,
        sum(o.amt_joker_eur)                                         as Joker_EUR,
        sum(o.amt_delivery_fee_eur)                                  as Delivery_Fee_EUR,
        -- Volume --
        sum(case when o.is_acquisition then o.order_qty else 0 end)  as NewCustomers, -- Number of first *successful* orders // first_order_all considers the first order regardless of its final status
        sum(o.order_qty)                                             as Orders
    from od_orders o
    group by 1,2,3,4,5,6,7,8,20;

--drop table if exists deliveries;
create temp table deliveries
distkey(delivery_date) as
    select
        de.rdbms_id,
        de.entity_display_name,
        de.city_id,
        o.zone_id,
        o.delivery_date,
        case
            when de.to_customer_time < 5*60.0 then '<05'
            when de.to_customer_time < 10*60.0 then '<10'
            when de.to_customer_time < 15*60.0 then '<15'
            when de.to_customer_time < 20*60.0 then '<20'
            when de.to_customer_time >= 20*60.0 then '>=20'
            else 'N/A' end                                           as DrivingTimeBucket,
        sum(de.to_customer_time)                                     as TotalDrivingTime,
        sum(case when de.to_customer_time is not null then 1 end)    as TotalDrivingsWithTime,
        sum(de.dropoff_distance_manhattan)                           as TotalDrivingDistance,
        /*sum(case when de.pickup <> de.dropoff then
            st_distancesphere(st_geomfromtext(de.pickup),
                st_makepoint(
                    st_x(st_geomfromtext(de.pickup)),
                    st_y(st_geomfromtext(de.dropoff))))
            + st_distancesphere(st_geomfromtext(de.dropoff),
                st_makepoint(
                    st_x(st_geomfromtext(de.pickup)),
                    st_y(st_geomfromtext(de.dropoff)))) end)         as TotalDrivingDistance,*/
        sum(case when de.pickup <> de.dropoff then 1 end)            as TotalDrivingsWithDistance,
        count(*)                                                     as Deliveries
    from dwh_redshift_logistic.v_clg_deliveries de
    inner join od_orders o on de.rdbms_id = o.rdbms_id and de.entity_display_name = o.entity_display_name and de.order_id = o.log_order_id
    group by 1,2,3,4,5,6;

--drop table if exists shifts;
create temp table shifts
distkey(shift_date)
as
    select
        s.rdbms_id,
        s.city_id,
        s.zone_id,
        s.created_date as shift_date,
        sum(s.actual_working_time) as ActualWorkingTimeInSec
    from dwh_redshift_logistic.v_clg_shifts s
    where s.created_date between current_date - 187 and current_date
    group by 1,2,3,4;

--drop table if exists distinct_data;
create temp table distinct_data
distkey(order_date)
as
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.order_date,
        count(distinct(analytical_customer_id)) as Distinct_Customers,
        count(distinct(restaurant_id)) as Distinct_Restaurants
    from od_orders o
    group by 1,2,3,4,5;

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
        count(distinct o.analytical_customer_id) as Week_Valid_Customers,
        sum(o.order_qty) as Week_Valid_Orders
    from od_orders o
    inner join dwh_il.dim_date d on o.order_date > d.iso_date - 7
        and o.order_date <= d.iso_date
        and d.iso_date between current_date - 187 and current_date + 7
    group by 1,2,3,4,5;

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
        case rest.source_id
            when 68 then 40 -- OnlinePizza to Foodora Sweden
            when 18 then 129 -- PedidosYa Panama to Appetito24, but order id does not seem to be matching for those ~600 orders
            else rest.source_id
        end as source_id,
        city.hurrier_city_id as city_id,
        hist.valid_at as date,
        count(distinct case when hist.is_online and hist.is_dh_delivery then hist.restaurant_id end) as number_of_od_restaurants,
        count(distinct case when hist.is_online then hist.restaurant_id end) as number_of_restaurants
    from dwh_il.dim_restaurant_history hist
    join dwh_il.dim_restaurant as rest on rest.source_id = hist.source_id and rest.restaurant_id = hist.restaurant_id
    join city_id_dictionary city on rest.city_id = city.backend_city_id and rest.source_id = city.source_id
    where rest.source_id > 0 and hist.is_online and hist.valid_at between current_date - 187 and current_date
    group by 1,2,3;

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
        o.date,
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
        d.totaldrivingswithdistance,
        d.deliveries,
        s.actualworkingtimeinsec,
        dd.distinct_customers,
        dd.distinct_restaurants,
        w.week_valid_customers,
        w.week_valid_orders,
        r.number_of_od_restaurants,
        r.number_of_restaurants
    from orders o
    left join deliveries d on o.rdbms_id = d.rdbms_id and o.entity_display_name = d.entity_display_name and o.city_id = d.city_id and o.zone_id = d.zone_id and o.date = d.delivery_date
    left join shifts s on o.rdbms_id = s.rdbms_id and o.city_id = s.city_id and o.zone_id = s.zone_id and o.date = s.shift_date
    left join distinct_data dd on o.rdbms_id = dd.rdbms_id and o.entity_display_name = dd.entity_display_name and o.city_id = dd.city_id and o.zone_id = dd.zone_id and o.date = dd.order_date
    left join weekly_frequency w on o.rdbms_id = w.rdbms_id and o.entity_display_name = w.entity_display_name and o.city_id = w.city_id and o.zone_id = w.zone_id and o.date = w.iso_date
    left join active_restaurants r on o.source_id = r.source_id and o.city_id = r.city_id and o.date = r.date
    left join dwh_il.dim_countries co on o.source_id = co.source_id
    left join dwh_redshift_logistic.v_clg_cities lc on o.rdbms_id = lc.rdbms_id and o.city_id = lc.city_id
    left join dwh_redshift_logistic.v_clg_zones z on o.rdbms_id = z.rdbms_id and o.city_id = z.city_id and o.zone_id = z.zone_id
    where co.is_active and o.date between current_date - 180 and current_date;

analyze dwh_bl.tableau_pricing_report predicate columns;