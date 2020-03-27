drop table if exists entity_to_company;
create temp TABLE entity_to_company as (
    select
        lo.rdbms_id,
        lo.entity_display_name,
        pc.dwh_country_id,
        case lo.entity_display_name
            when 'Amrest' then 0 -- Third-party deliveries for AmRest
            when 'Appetito24' then 57
            when 'FD - Sweden' then 27 -- Switched to Online Pizza which was rebranded as foodora Sweden on 2020-01-09
            when 'Boozer' then 34
            when 'Burger King - Singapore' then 45
            when 'Damejidlo' then 20
            when 'Deliveras' then 58 -- Not 'joinable' as of 2020-01-15 (DATA-3784)
            when 'eFood' then 2
            when 'FD Usehurrier - NL' then 36
            when 'Hungrig Sweden' then 65
            when 'KFC - Bulgaria' then 0 -- Third-party deliveries
            when 'Lieferheld' then 24
            when 'Mjam' then 28
            when 'Netpincer' then 51
            when 'Onlinepizza Sweden' then 27
            when 'On Demand Rider - Pakistan' then 0 -- Third-party deliveries
            when 'Otlob' then 55
            when 'Pauza' then 46
            when 'Pizza.de' then 23
            when 'Pizza-Online Finland' then 3
            when 'Vapiano' then 0 -- Third-party deliveries
            when 'Walmart - Canada' then 34
            when 'Yemeksepeti' then 21
            else case
                when lo.entity_display_name like 'CD - %' then 7
                when lo.entity_display_name like 'Carriage - %' then 54
                when lo.entity_display_name like 'CG - %' then 54
                when lo.entity_display_name like 'DN - %' then 47 -- 'DN - Bosnia and Herzegovina' not 'joinable' as of 2020-01-15 (DATA-3784), but data is present in BigQuery
                when lo.entity_display_name like 'FD - %' then 34
                when lo.entity_display_name like 'FP - %' then 45
                when lo.entity_display_name like 'Hip Menu - %' then 60 -- Deprecated on 2019-12-10, order_code is encrypted
                when lo.entity_display_name like 'Hungerstation - %' then 53
                when lo.entity_display_name like 'PY - %' then 6
                when lo.entity_display_name like 'TB - %' then 25
                when lo.entity_display_name like 'ZO - %' then 64
                end
            end dwh_company_id
    from dwh_redshift_logistic.v_clg_orders lo
    left join dwh_redshift_pd_il.dim_countries pc on lo.rdbms_id = pc.rdbms_id
    group by 1,2,3,4)

truncate table bi_global_pricing_dev.pricing_mapping_source_rdbms_entity;
create table bi_global_pricing_dev.pricing_mapping_source_rdbms_entity AS (
    select
        c.source_id,
        d.rdbms_id,
        d.entity_display_name
    from entity_to_company d
    left join dwh_il.dim_countries c using(dwh_country_id,dwh_company_id)
    where is_active
    order by entity_display_name);