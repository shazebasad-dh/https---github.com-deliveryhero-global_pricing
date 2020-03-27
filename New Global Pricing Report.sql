--set enable_result_cache_for_session to off;

drop table if exists run_time;
create temp table run_time as (select '1. Script started'::VARCHAR(50) event, getdate() run_time);

drop table bi_global_pricing_dev.tableau_pricing_report;
create table if not exists bi_global_pricing_dev.tableau_pricing_report
(
  management_entity_group         VARCHAR(30)     ENCODE lzo
  ,company_name                   VARCHAR(30)     ENCODE lzo
  ,country                        VARCHAR(30)     ENCODE lzo
  ,country_iso                    VARCHAR(2)      ENCODE lzo
  ,currency                       VARCHAR(3)      ENCODE lzo
  ,region                         VARCHAR(10)     ENCODE lzo
  ,entity_display_name            VARCHAR(50)     ENCODE lzo
  ,source_id                      INTEGER         ENCODE az64
  ,rdbms_id                       INTEGER         ENCODE az64
  ,city                           VARCHAR(50)     ENCODE lzo
  ,city_id                        INTEGER         ENCODE az64
  ,zone                           VARCHAR(50)     ENCODE lzo
  ,zone_id                        INTEGER         ENCODE az64
  ,date                           DATE            ENCODE az64
  ,nps_scores                     INTEGER         ENCODE az64
  ,nps_responses                  INTEGER         ENCODE az64
  ,df_lc                          NUMERIC(19,2)   ENCODE az64
  ,log_df_lc                      NUMERIC(36,2)   ENCODE az64
  ,paid_lc                        NUMERIC(36,2)   ENCODE az64
  ,cv_lc                          NUMERIC(36,2)   ENCODE az64
  ,voucher_dh_lc                  NUMERIC(36,2)   ENCODE az64
  ,voucher_other_lc               NUMERIC(36,2)   ENCODE az64
  ,discount_dh_lc                 NUMERIC(36,2)   ENCODE az64
  ,discount_other_lc              NUMERIC(36,2)   ENCODE az64
  ,revenue_dh_lc                  NUMERIC(36,2)   ENCODE az64
  ,commission_lc                  NUMERIC(36,2)   ENCODE az64
  ,joker_lc                       NUMERIC(36,2)   ENCODE az64
  ,delivery_fee_lc                NUMERIC(36,2)   ENCODE az64
  ,log_delivery_fee_lc            NUMERIC(36,2)   ENCODE az64
  ,df_eur                         NUMERIC(12,2)   ENCODE az64
  ,paid_eur                       NUMERIC(12,2)   ENCODE az64
  ,cv_eur                         NUMERIC(12,2)   ENCODE az64
  ,voucher_dh_eur                 NUMERIC(12,2)   ENCODE az64
  ,voucher_other_eur              NUMERIC(12,2)   ENCODE az64
  ,discount_dh_eur                NUMERIC(12,2)   ENCODE az64
  ,discount_other_eur             NUMERIC(12,2)   ENCODE az64
  ,revenue_dh_eur                 NUMERIC(12,2)   ENCODE az64
  ,commission_eur                 NUMERIC(12,2)   ENCODE az64
  ,joker_eur                      NUMERIC(12,2)   ENCODE az64
  ,delivery_fee_eur               NUMERIC(12,2)   ENCODE az64
  ,newcustomers                   INTEGER         ENCODE az64
  ,orders                         INTEGER         ENCODE az64
  ,drivingtimebucket              VARCHAR(5)      ENCODE lzo
  ,totaldrivingtime               INTEGER         ENCODE az64
  ,totaldrivingswithtime          INTEGER         ENCODE az64
  ,deliveries                     INTEGER         ENCODE az64
  ,actualworkingtimeinsec         INTEGER         ENCODE az64
  ,distinct_customers             INTEGER         ENCODE az64
  ,distinct_restaurants           INTEGER         ENCODE az64
  ,week_valid_customers           INTEGER         ENCODE az64
  ,week_valid_orders              INTEGER         ENCODE az64
  ,number_of_od_restaurants       INTEGER         ENCODE az64
  ,number_of_restaurants          INTEGER         ENCODE az64
) diststyle all;

insert into run_time (select '2. Report table created' event, getdate() run_time);

drop table if exists construct_orders;
create temp table construct_orders as (
    select
        c.source_id,
        d.iso_date as report_date
    from dwh_il.dim_countries as c
    cross join dwh_il.dim_date as d
    where c.is_active and d.iso_date between current_date - 187 and current_date);

insert into run_time (select '3. Temp table construct_orders created' event, getdate() run_time);

drop table if exists fct_orders;
create temp table fct_orders as (
    select
        case o.source_id
            when 68 then 40 -- OnlinePizza to Foodora Sweden
            when 18 then 129 -- PedidosYa Panama to Appetito24, but order id does not seem to be matching for those ~600 orders
            else o.source_id
        end as source_id,
        o.restaurant_id,
        o.analytical_customer_id,
        o.order_date::date,
        case
            when o.source_id in (39, 97, 143, 32) then o.order_number -- 39: Austria; 97: Hungary; 143: Sweden; 32: Turkey --> entities that only order_number can be used to join with platform_order_code
            when o.source_id = 119 then 'CG-1-' + o.order_id -- Kuwait
            when o.source_id = 120 then 'CG-2-' + o.order_id -- Bahrain
            when o.source_id = 121 then 'CG-3-' + o.order_id -- CG United Arab Emirates
            when o.source_id = 122 then 'CG-4-' + o.order_id -- Qatar
            when o.source_id = 124 then 'CG-5-' + o.order_id -- Saudi Arabia
            when o.source_id = 142 then 'zo-' + o.order_id -- Zomato United Arab Emirates
            else o.order_id
        end order_id,
        o.is_acquisition,
        o.order_qty,
        --nps--
        nps.nps,
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
    left join dwh_il.fct_nps_ao nps on o.order_id = nps.order_id and o.source_id = nps.source_id
    inner join construct_orders c on o.order_date::date = c.report_date and o.source_id = c.source_id
    where not (o.is_cancelled or o.is_declined or o.is_failed));

insert into run_time (select '4. Temp table fct_orders created' event, getdate() run_time);

drop table if exists construct_logistic;
create temp table construct_logistic as (
    select
        d.iso_date as report_date,
        lo.rdbms_id,
        lo.entity_display_name
    from (select rdbms_id, entity_display_name from dwh_redshift_logistic.v_clg_orders group by 1,2) lo
    cross join dwh_il.dim_date as d
    where d.iso_date between current_date - 187 and current_date);

insert into run_time (select '6. Temp table construct_logistic created' event, getdate() run_time);

drop table if exists log_orders;
create temp table log_orders as (
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
        lo.platform_order_code,
        lo.order_id as log_order_id,
        lo.order_placed_at::date as delivery_date,
        lo.delivery_fee/100 as log_df_lc
    from dwh_redshift_logistic.v_clg_orders lo
    left join bi_global_pricing_dev.pricing_mapping_source_rdbms_entity m on m.rdbms_id = lo.rdbms_id and m.entity_display_name = lo.entity_display_name
    inner join construct_logistic c on lo.entity_display_name = c.entity_display_name and lo.rdbms_id = c.rdbms_id and lo.order_placed_at::date = c.report_date
    where lo.order_status = 'completed');

insert into run_time (select '7. Temp table log_orders created' event, getdate() run_time);

drop table if exists od_orders;
create temp table od_orders as (
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
        o.order_date,
        lo.delivery_date,
        o.is_acquisition,
        o.order_qty,
        --nps--
        o.nps,
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
    from log_orders lo
    inner join fct_orders o on lo.source_id = o.source_id and lo.platform_order_code = o.order_id
    left join dwh_il.dim_restaurant r on o.restaurant_id = r.restaurant_id);

insert into run_time (select '8. Temp table od_orders created' event, getdate() run_time);

drop table if exists orders;
create temp table orders as (
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
        sum(o.order_qty)                                             as Orders,
        -- NPS --
        sum(case when o.nps >= 9 then 1.0
            when o.nps <= 6 then -1.0 else 0 end)                    as NPS_Scores,
        sum(case when o.nps is not null then 1 end)                  as NPS_Responses
    from od_orders o
    group by 1,2,3,4,5,6,7,8,20);

insert into run_time (select '9. Temp table orders created' event, getdate() run_time);

drop table if exists deliveries;
create temp table deliveries as (
    select
        de.rdbms_id, de.entity_display_name, de.city_id, o.zone_id, o.delivery_date,
        case
            when de.to_customer_time < 5*60.0 then '<05'
            when de.to_customer_time < 10*60.0 then '<10'
            when de.to_customer_time < 15*60.0 then '<15'
            when de.to_customer_time < 20*60.0 then '<20'
            when de.to_customer_time >= 20*60.0 then '⩾20'
            else 'N/A' end                                           as DrivingTimeBucket,
        sum(de.to_customer_time)                                     as TotalDrivingTime,
        sum(case when de.to_customer_time is not null then 1 end)    as TotalDrivingsWithTime,
        count(*)                                                     as Deliveries
    from dwh_redshift_logistic.v_clg_deliveries de
    inner join od_orders o on de.rdbms_id = o.rdbms_id and de.entity_display_name = o.entity_display_name and de.order_id = o.log_order_id
    group by 1,2,3,4,5,6);

insert into run_time (select '10. Temp table deliveries created' event, getdate() run_time);

drop table if exists shifts;
create temp table shifts as (
    select
        s.rdbms_id,
        s.city_id,
        s.zone_id,
        s.created_date as shift_date,
        sum(s.actual_working_time) as ActualWorkingTimeInSec
    from dwh_redshift_logistic.v_clg_shifts s
    inner join (select rdbms_id, report_date from construct_logistic c group by 1,2) c on s.rdbms_id = c.rdbms_id and s.created_date = c.report_date
    group by 1,2,3,4);

insert into run_time (select '11. Temp table shifts created' event, getdate() run_time);

drop table if exists distinct_data;
create temp table distinct_data as (
    select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.order_date,
        count(distinct(analytical_customer_id)) as Distinct_Customers,
        count(distinct(restaurant_id)) as Distinct_Restaurants
    from od_orders o
    group by 1,2,3,4,5);

insert into run_time (select '12. Temp table distinct_data created' event, getdate() run_time);

drop table if exists weekly_frequency;
create temp table weekly_frequency as (
   select
        o.rdbms_id,
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        d.iso_date,
        count(distinct o.analytical_customer_id) as Week_Valid_Customers,
        sum(o.order_qty) as Week_Valid_Orders
    from dwh_il.dim_date d
    inner join od_orders o on o.order_date > d.iso_date - 7 and o.order_date <= d.iso_date
    inner join construct_logistic c on o.rdbms_id = c.rdbms_id and o.entity_display_name = c.entity_display_name and d.iso_date = c.report_date
    group by 1,2,3,4,5);

insert into run_time (select '13. Temp table weekly_frequency created' event, getdate() run_time);

drop table if exists city_id_dictionary;
create temp table city_id_dictionary as (
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
            group by 1,2,3
        order by 1 asc, 3 asc, 5 desc)
    where rank = 1);

insert into run_time (select '14. Temp table city_dictionary created' event, getdate() run_time);

drop table if exists active_restaurants;
create temp table active_restaurants as (
    select
        rest.source_id,
        city.hurrier_city_id as city_id,
        hist.valid_at as date,
        count(distinct case when hist.is_online and hist.is_dh_delivery then hist.restaurant_id end) as number_of_od_restaurants,
        count(distinct case when hist.is_online then hist.restaurant_id end) as number_of_restaurants
    from dwh_il.dim_restaurant_history hist
    join dwh_il.dim_restaurant as rest on rest.source_id = hist.source_id and rest.restaurant_id = hist.restaurant_id
    join city_id_dictionary city on rest.city_id = city.backend_city_id and rest.source_id = city.source_id
    inner join construct_orders co on rest.source_id = co.source_id and hist.valid_at = co.report_date
    where rest.source_id > 0 and hist.is_online
    group by 1,2,3
    order by hist.valid_at);

insert into run_time (select '15. Temp table active_restaurants created' event, getdate() run_time);

truncate table bi_global_pricing_dev.tableau_pricing_report;
insert into bi_global_pricing_dev.tableau_pricing_report (
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
        o.nps_scores,
        o.nps_responses,
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
    inner join (select dateadd('day',7, report_date) as date from construct_orders group by 1) dt on o.date = dt.date);

insert into run_time (select '17. Temp table into report inserted' event, getdate() run_time);

grant all on bi_global_pricing_dev.tableau_pricing_report to group bi_global_pricing;
grant all on bi_global_pricing_dev.tableau_pricing_report to group bi_global;
grant all on bi_global_pricing_dev.tableau_pricing_report to group bi_foodora;
grant all on bi_global_pricing_dev.tableau_pricing_report to tableau_global;