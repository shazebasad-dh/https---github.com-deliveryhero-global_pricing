drop table if exists fct_orders;
create temp table fct_orders as (
    select
        co.management_entity_group, co.company_name, co.dwh_company_id, co.common_name, co.country_iso, co.dwh_country_id, co.currency_code,
        case when o.source_id = 68 then 40 else o.source_id end source_id, -- OnlinePizza switched to Foodora Sweden on 2019-12-06 12:34:05
        coalesce(o.city_name_english, o.city_name_local) backend_city, o.city_id backend_city_id, o.restaurant_id, o.analytical_customer_id, o.order_date::date,
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
        nps.nps,
        o.amt_paid_eur, o.amt_paid_lc,
        --o.amt_gmv_eur, o.amt_gmv_lc, -- redundant as they are exactly like amt_paid
        o.amt_cv_eur, o.amt_cv_lc,
        o.amt_commission_eur, o.amt_commission_lc,
        o.amt_joker_eur, o.amt_joker_lc,
        o.amt_delivery_fee_eur, o.amt_delivery_fee_lc,
        o.amt_dh_revenue_eur, o.amt_dh_revenue_lc,
        o.amt_discount_dh_eur, o.amt_discount_dh_lc,
        o.amt_discount_other_eur, o.amt_discount_other_lc,
        o.amt_voucher_dh_eur, o.amt_voucher_dh_lc,
        o.amt_voucher_other_eur, o.amt_voucher_other_lc,
        o.is_acquisition, o.is_sent, o.order_qty
    from (select o.*, c.city_name_english, c.city_name_local from dwh_il.ranked_fct_order o
            left join dwh_il.dim_restaurant r on o.source_id = r.source_id and o.restaurant_id = r.restaurant_id
            left join dwh_il.dim_city c on r.source_id = c.source_id and r.city_id = c.city_id
        where not (o.is_cancelled or o.is_declined or o.is_failed) and o.source_id > 0 and o.order_date between current_date - 90 - 7 and current_date) o
    left join dwh_il.dim_countries co on o.source_id = co.source_id
    left join dwh_il.fct_nps_ao nps on o.order_id = nps.order_id and o.source_id = nps.source_id);

drop table if exists log_orders;
create temp table log_orders as (
   select
        --region, company--
        lo.platform,
        entity_display_name,
        case lo.entity_display_name
            when 'Appetito24' THEN 57
            when 'Boozer' THEN 34
            when 'Burger King - Singapore' THEN 45
            when 'CD - Colombia' THEN 7
            when 'CG - Bahrain' THEN 54
            when 'CG - Kuwait' THEN 54
            when 'CG - Qatar' THEN 54
            when 'CG - Saudi Arabia' THEN 54
            when 'CG - UAE' THEN 54
            when 'Carriage - Egypt' THEN 54
            when 'DN - Serbia' THEN 47
            when 'DN - Bosnia and Herzegovina' THEN 47 -- Not 'joinable' as of 2020-01-15 (DATA-3784), but data is present in BigQuery
            when 'Damejidlo' THEN 20
            when 'Deliveras' THEN 58 -- Not 'joinable' as of 2020-01-15 (DATA-3784)
            when 'FD - Austria' THEN 34 -- Deprecated on 2019-11-26
            when 'FD - Canada' THEN 34
            when 'FD - Finland' THEN 34
            when 'FD - Norway' THEN 34
            when 'FD - Sweden' THEN 27 -- Switched to Online Pizza which was rebranded as foodora Sweden on 2020-01-09
            when 'FP - Bangladesh' THEN 45
            when 'FP - Bulgaria' THEN 45
            when 'FP - Cambodia' THEN 45
            when 'FP - Hong Kong' THEN 45
            when 'FP - Laos' THEN 45
            when 'FP - Malaysia' THEN 45
            when 'FP - Myanmar' THEN 45
            when 'FP - Pakistan' THEN 45
            when 'FP - Philippines' THEN 45
            when 'FP - Romania' THEN 45
            when 'FP - Singapore' THEN 45
            when 'FP - Taiwan' THEN 45
            when 'FP - Thailand' THEN 45
            when 'Hip Menu - Romania' THEN 60 -- Deprecated on 2019-12-10, order_code is encrypted
            when 'Hungerstation - Bahrain' THEN 53
            when 'Hungerstation - SA' THEN 53
            when 'Hungrig Sweden' THEN 65
            when 'Mjam' THEN 28
            when 'Netpincer' THEN 51
            when 'Onlinepizza Sweden' THEN 27
            when 'Otlob' THEN 55
            when 'Pauza' THEN 46
            when 'Pizza-Online Finland' THEN 3
            when 'PY - Argentina' THEN 6
            when 'PY - Bolivia' THEN 6
            when 'PY - Chile' THEN 6
            when 'PY - Dominican Republic' THEN 6
            when 'PY - Paraguay' THEN 6
            when 'PY - Uruguay' THEN 6
            when 'TB - Bahrain' THEN 25
            when 'TB - Jordan' THEN 25
            when 'TB - Kuwait' THEN 25
            when 'TB - Oman' THEN 25
            when 'TB - Qatar' THEN 25
            when 'TB - UAE' THEN 25
            when 'Walmart - Canada' THEN 34
            when 'Yemeksepeti' THEN 21
            when 'ZO - UAE' THEN 64
            end company_id,
        --country--
        case when lo.rdbms_id = 88 then 144 else lo.rdbms_id end rdbms_id, -- 88 (foodora Sweden) was shut down and replaced by 144 (Onlinepizza Sweden) which then was rebranded to Foodora Sweden on 2020-01-09
        lo.country_code,
        --city, zone--
        lo.city_id, coalesce(lo.zone_id,0) zone_id, -- Adjustment to the orders with no zone assigned
        --vendor--
        v.vendor_name, v.vendor_code, v.vendor_id,
        --orders data--
        lo.platform_order_code, lo.order_placed_at::date as delivery_date, lo.order_id, lo.delivery_fee/100 as log_df_lc
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_redshift_logistic.v_clg_vendors v using(rdbms_id, city_id, vendor_id)
    where
        lo.order_status = 'completed');

drop table if exists od_orders;
create temp table od_orders as (
    select
        --region, company--
        c.region, o.management_entity_group, o.company_name, lo.entity_display_name,
        --country--
        o.common_name, o.country_iso, lo.country_code, o.source_id, lo.rdbms_id, o.currency_code,
        --city--
        lc.name as city_name, lc.city_id, o.backend_city, o.backend_city_id,
        --zone--
        z.name as zone_name, z.zone_id as zone_id,
        --orders data--
        lo.platform_order_code, o.restaurant_id, lo.vendor_name, lo.vendor_code, lo.vendor_id, o.analytical_customer_id, o.order_date, lo.delivery_date, o.order_id, lo.order_id log_order_id,
        o.nps,
        o.amt_paid_eur, o.amt_paid_lc,
        o.amt_cv_eur, o.amt_cv_lc,
        o.amt_commission_eur, o.amt_commission_lc,
        o.amt_joker_eur, o.amt_joker_lc,
        o.amt_delivery_fee_eur, o.amt_delivery_fee_lc, lo.log_df_lc,
        o.amt_dh_revenue_eur, o.amt_dh_revenue_lc,
        o.amt_discount_dh_eur, o.amt_discount_dh_lc,
        o.amt_discount_other_eur, o.amt_discount_other_lc,
        o.amt_voucher_dh_eur, o.amt_voucher_dh_lc,
        o.amt_voucher_other_eur, o.amt_voucher_other_lc,
        o.is_acquisition, o.is_sent, o.order_qty
    from log_orders lo
    left join dwh_redshift_pd_il.dim_countries c on lo.rdbms_id = c.rdbms_id
    left join dwh_redshift_logistic.v_clg_cities lc on lo.rdbms_id = lc.rdbms_id and lo.city_id = lc.city_id and lo.country_code = lc.country_code
    left join dwh_redshift_logistic.v_clg_zones z on lo.rdbms_id = z.rdbms_id and lo.city_id = z.city_id and lo.zone_id = z.zone_id and lo.country_code = z.country_code
    inner join fct_orders o on c.dwh_country_id = o.dwh_country_id and lo.company_id = o.dwh_company_id and lo.platform_order_code = o.order_id);

drop table if exists deliveries_filtered;
create temp table deliveries_filtered as (
    select
        de.entity_display_name, o.city_id, o.zone_id, o.order_date, o.delivery_date, o.log_order_id, de.to_customer_time
    from dwh_redshift_logistic.v_clg_deliveries de
    inner join od_orders o on de.entity_display_name = o.entity_display_name and de.order_id = o.log_order_id and de.country_code = o.country_code);

drop table if exists orders;
create temp table orders as (
select
        o.region                                                     as Region,
        o.management_entity_group                                    as Management_Entity_Group,
        o.company_name                                               as Company_Name,
        o.entity_display_name                                        as Entity_Display_Name,
        o.common_name                                                as Country,
        o.country_iso                                                as Country_Iso,
        o.currency_code                                              as Currency,
        o.source_id                                                  as Source_Id,
        o.rdbms_id                                                   as Rdbms_Id,
        o.city_name                                                  as City,
        o.city_id                                                    as City_Id,
        o.zone_name                                                  as Zone,
        o.zone_id                                                    as Zone_Id,
        o.order_date                                                 as Date,
        sum(case when o.nps >= 9 then 1.0
            when o.nps <= 6 then -1.0 else 0 end)                    as nps_scores,
        sum(case when o.nps is not null then 1 end)                  as nps_responses,
        -- Revenue in LC --
        o.amt_delivery_fee_lc                                        as DF_LC,
        o.log_df_lc                                                  as Log_DF_LC,
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
    where date >= current_date - 90
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18,30);

drop table if exists deliveries;
create temp table deliveries as (
    select
        de.entity_display_name, de.city_id, de.zone_id,
        de.delivery_date,
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
    from deliveries_filtered de
    where delivery_date >= current_date - 90
    group by 1,2,3,4,5);

drop table if exists shifts;
create temp table shifts as (
    select
        s.rdbms_id, s.city_id, s.zone_id, s.created_date as shift_date, sum(s.actual_working_time) as ActualWorkingTimeInSec
    from dwh_redshift_logistic.v_clg_shifts s
    where s.created_date >= current_date - 90
    group by 1,2,3,4);

drop table if exists distinct_data;
create temp table distinct_data as (
    select
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        o.order_date,
        count(distinct(analytical_customer_id)) as Distinct_Customers,
        count(distinct(restaurant_id)) as Distinct_Restaurants
    from od_orders o
    group by 1,2,3,4);

drop table if exists weekly_frequency;
create temp table weekly_frequency as (
   select
        o.entity_display_name,
        o.city_id,
        o.zone_id,
        d.iso_date,
        count(distinct o.analytical_customer_id) as Week_Valid_Customers,
        sum(o.order_qty) as Week_Valid_Orders
    from dwh_il.dim_date d
    inner join od_orders o on o.order_date > d.iso_date - 7 and o.order_date <= d.iso_date
    where iso_date >= current_date - 90
    group by 1,2,3,4);

drop table if exists pricing_report;
create temp table pricing_report as (
    select
        o.*,
        d.DrivingTimeBucket, d.TotalDrivingTime, d.TotalDrivingsWithTime, d.Deliveries,
        s.ActualWorkingTimeInSec,
        dd.Distinct_Customers, dd.Distinct_Restaurants,
        w.Week_Valid_Customers, w.Week_Valid_Orders
    from orders o
    left join deliveries d on o.entity_display_name = d.entity_display_name and o.city_id = d.city_id and o.zone_id = d.zone_id and o.date = d.delivery_date
    left join shifts s on o.rdbms_id = s.rdbms_id and o.city_id = s.city_id and o.zone_id = s.zone_id and o.date = s.shift_date
    left join distinct_data dd on o.entity_display_name = dd.entity_display_name and o.city_id = dd.city_id and o.zone_id = dd.zone_id and o.date = dd.order_date
    left join weekly_frequency w on o.entity_display_name = w.entity_display_name and o.city_id = w.city_id and o.zone_id = w.zone_id and o.date = w.iso_date);