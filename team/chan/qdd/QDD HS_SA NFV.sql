
--Variable setting
DECLARE CtryCode STRING;
DECLARE DateIndicator DATE;
DECLARE vertical STRING;
 
--Definite Inputs
SET CtryCode = 'sa';
SET DateIndicator = '2022-01-01';
SET vertical = "shops";

    CREATE OR REPLACE EXTERNAL TABLE `dh-logistics-product-ops.pricing._sa_rdf_vendors`
        (
        year STRING,
        month FLOAT64,
        offer_type STRING,
        restaurant_max_charge FLOAT64,
        delivery_fee FLOAT64
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1-u6bmK6NpeRZyPEGmu4NKCj-ia0S1AWWvDciJG_wtdQ/edit#gid=1359779822"],
        sheet_range="data!A1:E7",
        skip_leading_rows=1
    );


    CREATE OR REPLACE TEMP TABLE pre_staging_orders AS  
    WITH 
        average_commission AS(
        SELECT
        vendor_id,
        avg(gfv_local) avg_gfv,
        avg(commission_local) avg_commission
        FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
        WHERE 1=1
        AND UPPER(country_code) = CtryCode
        -- AND vertical_type = 'restaurants'
        AND created_date BETWEEN current_date()-84 AND current_date()-2
        AND commission_local IS NOT NULL
        AND commission_local <> 0
        AND gfv_local IS NOT NULL
        AND gfv_local <> 0
        GROUP BY 1
    )
    
    , commission_percentage AS(
        SELECT
        vendor_id,
        avg_commission/avg_gfv commission_percentage
        FROM average_commission
    )
    
    , rdf_offers as (
        select
        ARRAY_AGG(delivery_fee) as rdf_agg
        from `dh-logistics-product-ops.pricing._sa_rdf_vendors`
      )

    , rdf_vendor_contributions as (
        select
        delivery_fee
        , restaurant_max_charge as rdf_vendor_contribution
        from `dh-logistics-product-ops.pricing._sa_rdf_vendors`

    )


    , load_dps_sessions as (
        select
        platform_order_code
        , country_code
        , vendor_price_scheme_type

        from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
        where created_date >= DateIndicator
        AND country_code = CtryCode
    )
    
    , real_df AS(
        SELECT 
        map.test_name
        , map.variant
        , map.order_id
        , vertical_type
        , platform_order_code
        , CASE 
            WHEN os.content.delivery.delivery_fee - os.content.customer.payment.discount IN UNNEST(rdf_agg) 
                AND os.content.customer.payment.discount > 0
                THEN os.content.delivery.delivery_fee - os.content.customer.payment.discount 
            ELSE delivery_fee_local
        END as delivery_fee_local

        ,CASE 
            WHEN os.content.delivery.delivery_fee - os.content.customer.payment.discount IN UNNEST(rdf_agg) 
                AND os.content.customer.payment.discount > 0  
                THEN TRUE
            ELSE FALSE
        END as is_rdf_order

        , map.dps_delivery_fee_local
        , map.dps_surge_fee_local
        , map.dps_minimum_order_value_local
        , map.gfv_local
        , map.gfv_eur
        , CASE
            WHEN map.commission_local IS NULL THEN gfv_local * cm.commission_percentage
            WHEN map.commission_local = 0 THEN gfv_local * cm.commission_percentage
            ELSE map.commission_local 
        END AS commission_local
        , map.delivery_costs_local
        , map.to_customer_time
        , map.travel_time
        , map.delivery_distance
        , map.mean_delay
        , map.created_date
        , country_code
        , map.service_fee_local
        , target_group
        , vendor_price_scheme_type

        FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` map, rdf_offers

        LEFT JOIN commission_percentage cm    
            ON map.vendor_id = cm.vendor_id

        LEFT JOIN load_dps_sessions
          using(platform_order_code, country_code)

        LEFT JOIN `fulfillment-dwh-production.curated_data_shared_data_stream.order_stream` os
            ON map.platform_order_code = os.content.order_id 
            AND map.entity_id = os.content.global_entity_id 
            AND os.created_date >= DateIndicator

        WHERE 1=1
        AND country_code = CtryCode
        AND variant NOT IN ('Original') 
        AND variant IS NOT NULL
        AND map.created_date >=  DateIndicator
        AND test_name NOT LIKE "%miscon%"
        AND delivery_distance IS NOT NULL
        AND travel_time IS NOT NULL    
        )

    , add_rdf_contribution as (
        select
        real_df.*
        , IFNULL(rdf_vendor_contribution,0) as rdf_vendor_contribution
        from real_df
        LEFT JOIN rdf_vendor_contributions
            on real_df.delivery_fee_local = rdf_vendor_contributions.delivery_fee
            AND real_df.is_rdf_order = TRUE

    )

    select
    *
    from add_rdf_contribution;


    IF vertical = "food" THEN
      # load only food tests
      # load only restaurants orders
      EXECUTE IMMEDIATE(

        """
        CREATE OR REPLACE TEMP TABLE pre_staging_orders AS
        select
        stg.*
        from pre_staging_orders stg
        WHERE vertical_type = "restaurants"
        """
      );

    ELSE
      # load only shops tests
      # load only shops orders
      EXECUTE IMMEDIATE(

        """
        CREATE OR REPLACE TEMP TABLE pre_staging_orders AS
        select
        stg.*
        from pre_staging_orders stg
        WHERE vertical_type <> "restaurants"
        """
      );
    END IF;


    CREATE OR REPLACE TEMP TABLE staging_orders AS  

    WITH orders AS (
        SELECT
        test_name
        , variant
        , target_group
        , COUNT(DISTINCT platform_order_code) AS Order_qty
        , COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" then platform_order_code end) as Campaign_Orders
        , COUNT(DISTINCT CASE WHEN is_rdf_order then platform_order_code end) as RDF_Orders
        , SUM(delivery_fee_local) AS delivery_fee_local
        , SUM(dps_delivery_fee_local) AS dps_delivery_fee_local
        , SUM(dps_surge_fee_local) AS dps_surge_fee_local
        , SUM(dps_minimum_order_value_local) AS dps_minimum_order_value_local
        , SUM(gfv_local) AS gfv_local
        , SUM(gfv_eur) AS gfv_eur
        , SUM(commission_local) AS commission_local
        , SUM(delivery_costs_local) AS delivery_costs_local
        , SUM(to_customer_time) AS to_customer_time
        , SUM(travel_time) AS travel_time
        , SUM(delivery_distance) AS delivery_distance
        , SUM(mean_delay) AS mean_delay
        , SUM(service_fee_local) as service_fee_local
        , sum(rdf_vendor_contribution) as rdf_vendor_contribution
        , MAX(created_date) AS created_date
        FROM pre_staging_orders
        group by 1,2,3
    )

    , treatment_level_kpi as (
        select
        test_name
        , variant
        , "Treatment" as target_group
        , sum(Order_qty) Order_qty
        , sum(Campaign_Orders) Campaign_Orders
        , SUM(RDF_Orders) as RDF_Orders
        , SUM(delivery_fee_local) AS delivery_fee_local
        , SUM(dps_delivery_fee_local) AS dps_delivery_fee_local
        , SUM(dps_surge_fee_local) AS dps_surge_fee_local
        , SUM(dps_minimum_order_value_local) AS dps_minimum_order_value_local
        , SUM(gfv_local) AS gfv_local
        , SUM(gfv_eur) AS gfv_eur
        , SUM(commission_local) AS commission_local
        , SUM(delivery_costs_local) AS delivery_costs_local
        , SUM(to_customer_time) AS to_customer_time
        , SUM(travel_time) AS travel_time
        , SUM(delivery_distance) AS delivery_distance
        , SUM(mean_delay) AS mean_delay
        , SUM(service_fee_local) as service_fee_local
        , sum(rdf_vendor_contribution) as rdf_vendor_contribution
        , MAX(created_date) AS created_date

        from orders
        where target_group is not null
        group by 1,2,3
    )

    , all_level_metric as (
        select
        test_name
        , variant
        , "All" as target_group
        , sum(Order_qty) Order_qty
        , sum(Campaign_Orders) Campaign_Orders
        , SUM(RDF_Orders) as RDF_Orders
        , SUM(delivery_fee_local) AS delivery_fee_local
        , SUM(dps_delivery_fee_local) AS dps_delivery_fee_local
        , SUM(dps_surge_fee_local) AS dps_surge_fee_local
        , SUM(dps_minimum_order_value_local) AS dps_minimum_order_value_local
        , SUM(gfv_local) AS gfv_local
        , SUM(gfv_eur) AS gfv_eur
        , SUM(commission_local) AS commission_local
        , SUM(delivery_costs_local) AS delivery_costs_local
        , SUM(to_customer_time) AS to_customer_time
        , SUM(travel_time) AS travel_time
        , SUM(delivery_distance) AS delivery_distance
        , SUM(mean_delay) AS mean_delay
        , SUM(service_fee_local) as service_fee_local
        , sum(rdf_vendor_contribution) as rdf_vendor_contribution
        , MAX(created_date) AS created_date

        from orders
        group by 1,2,3
    )

    , join_order_metrics as (
        (
            select
            *
            from orders
            where target_group is not null 
        )

        UNION ALL

        (
            select
            *
            from treatment_level_kpi
        )

        UNION ALL

        (
            select
            *
            from all_level_metric
        )
    )

    ---- Users using ga_sessions

    , session_test_data as (
        select distinct
        test_id as experiment_id
        , test_name
        , entity_id as global_entity_id
        , test_start_date
        , IFNULL(test_end_date, CURRENT_TIMESTAMP()) as test_end_date

        from `fulfillment-dwh-production.cl.dps_experiment_setups`
        where country_code = CtryCode
        and date(test_start_date) >= date_add(DateIndicator, interval 8 month)
    )

    , unique_list_of_users as (

        select distinct
        test_name
        , s.global_entity_id
        , date(test_start_date) as test_start_date
        , date(test_end_date) as test_end_date
        , fullvisitor_id
        , sessions.experiment_id
        , sessions.variant
        , sessions.perseus_client_id

        from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` ga
        INNER JOIN session_test_data s
        ON ga.sessions.experiment_id = s.experiment_id
        AND  ga.entity_id = s.global_entity_id
        where TRUE
        AND sessions.variant <> "Original"
        AND sessions.variant IS NOT NULL
        AND country_code = CtryCode
        and created_date >= date_add(DateIndicator, interval 8 month)
    )

    , aggregate_cvr_metrics as (
    select
    test_name
    , variant
    , "All" as target_group
    , COUNT(DISTINCT session_id) AS total_sessions

    , safe_divide(
        NULLIF(SUM (mCVR1), 0)
        , NULLIF(COUNT (mCVR1),0)
        ) AS mCVR1

    ,  safe_divide(
        NULLIF(SUM (mCVR2), 0)
        , NULLIF(COUNT (mCVR2),0)
        ) AS mCVR2

    ,  safe_divide(
        NULLIF(SUM (mCVR3), 0)
        , NULLIF(COUNT (mCVR3),0)
        ) AS mCVR3

    ,  safe_divide(
        NULLIF(SUM (mCVR4), 0)
        , NULLIF(COUNT (mCVR4),0)
        ) AS mCVR4

    ,  safe_divide(
        COUNT(DISTINCT CASE WHEN has_order = 1 THEN session_id END)
        , COUNT(DISTINCT session_id)
        ) AS CVR

    from `fulfillment-dwh-production.curated_data_shared_product_analytics.ga_sessions` ga_sessions
    INNER JOIN unique_list_of_users
        ON ga_sessions.fullvisitor_id = unique_list_of_users.fullvisitor_id
        AND ga_sessions.global_entity_id = unique_list_of_users.global_entity_id
        AND partition_date BETWEEN test_start_date and test_end_date
    where partition_date >= date_add(DateIndicator, interval 8 month)
    AND dh_brand = "hungerstation"
    AND data_source = "app"
    AND ga_sessions.global_entity_id = "HS_SA"
    ---- Bug in Android Versions that prevented us to track session data
    ----https://deliveryhero.slack.com/archives/C03V0040SC9/p1672828786018059
    AND (
        CASE 
            WHEN (partition_date BETWEEN "2022-12-01" AND "2023-01-12") 
            AND frontend_client_type = "Android" 
            AND app_browser IN ("8.0.102", "8.0.103")
            THEN FALSE
            ELSE TRUE
        END
        )
    group by 1,2,3

    )
-----------


    , users AS (
        SELECT 
        test_name
        , variant
        , target_group
        , SUM(session_id_count) AS total_sessions
        , SUM(users_count_per_test) AS Distinct_users
        , SUM(transaction_no_count) AS transaction_no_count
        , SUM(list_menu_count) AS list_menu_count
        , SUM(shop_list_no_count) as shop_list_no_count
        , sum(shop_menu_no_count) as shop_menu_no_count
        , SUM(menu_checkout_count) AS menu_checkout_count
        , sum(checkout_no_count) as checkout_no_count
        , SUM(checkout_transaction_count) AS checkout_transaction_count
        , MIN(created_date) AS min_date
        , MAX(created_date) AS max_date


        FROM `fulfillment-dwh-production.rl.dps_ab_test_dashboard_cvr_v2`
        WHERE 1=1
        AND country_code = CtryCode
        AND variant NOT IN ('Original')
        AND variant IS NOT NULL
        AND target_group IS NOT NULL 
        AND test_name NOT LIKE "%iscon%"
        AND created_date >= DateIndicator
        GROUP BY 1,2,3
    )

    -- , users_treatment_level as (


    --     SELECT 
    --     test_name
    --     , variant
    --     , "Treatment" as target_group
    --     , SUM(total_sessions) AS total_sessions
    --     , SUM(Distinct_users) AS Distinct_users
    --     , SUM(transaction_no_count) AS transaction_no_count
    --     , SUM(list_menu_count) AS list_menu_count
    --     , SUM(shop_list_no_count) as shop_list_no_count
    --     , sum(shop_menu_no_count) as shop_menu_no_count
    --     , SUM(menu_checkout_count) AS menu_checkout_count
    --     , sum(checkout_no_count) as checkout_no_count
    --     , SUM(checkout_transaction_count) AS checkout_transaction_count
    --     , MIN(min_date) AS min_date
    --     , MAX(max_date) AS max_date

    --     from users
    --     where target_group <> "All"
    --     GROUP BY 1,2,3
    -- )

    , correct_user_distinct_users as (
        select
        test_name
        , variant 
        , Distinct_users

        from users
        where target_group = "All"

    )

    -- , union_user_metrics as (
    --     (
    --         select
    --         *
    --         from users
    --     )

    --     UNION ALL
    --     (
    --         select
    --         *
    --         from users_treatment_level
    --     )
    -- )

    , join_users_to_orders as (
        select
        o.*
        , s.* except(test_name, variant, target_group)
        , d.Distinct_users
        from join_order_metrics o
        LEFT JOIN  aggregate_cvr_metrics s
            using(test_name, variant, target_group)
        LEFT JOIN correct_user_distinct_users d
            using(test_name, variant)

    )

    , calculate_avg_metrics as (
        select
        concat(test_name, variant, target_group) as code
        ,  test_name
        ,  variant
        ,  target_group
        ,  Order_qty as Orders
        ,  Campaign_Orders
        ,  RDF_Orders
        ,  safe_divide(delivery_fee_local, Order_qty) AS Avg_Paid_DF
        ,  safe_divide(dps_delivery_fee_local, Order_qty) AS Avg_DPS_DF
        ,  safe_divide(dps_surge_fee_local, Order_qty) AS Avg_Surge_Fee
        ,  safe_divide(dps_minimum_order_value_local, Order_qty) AS AVG_MOV
        ,  safe_divide(gfv_local, Order_qty) AS AVG_FV
        ,  safe_divide(gfv_eur, Order_qty) AS AVG_FV_EUR
        ,  safe_divide(commission_local, Order_qty) AS AVG_Commission
        ,  safe_divide(delivery_costs_local, Order_qty) AS AVG_Delivery_Costs
        ,  safe_divide(to_customer_time, Order_qty) AS AVG_To_Customer_Time
        ,  safe_divide(travel_time, Order_qty) AS AVG_Travel_Time
        ,  safe_divide(delivery_distance, Order_qty) AS AVG_Manhattan_Distance
        ,  safe_divide(mean_delay, Order_qty) AS AVG_Fleet_Delay
        ,  safe_divide(service_fee_local, Order_qty) as AVG_Service_Fee
        ,  safe_divide(rdf_vendor_contribution, Order_qty) as AVG_rdf_vendor_contribution
        ,  created_date as max_order_date
        ,  total_sessions
        ,  Distinct_users
        ,  CVR
        ,  mCVR1
        ,  mCVR2
        ,  mCVR3
        ,  mCVR4
        -- ,  safe_divide(transaction_no_count, total_sessions) AS CVR
        -- ,  safe_divide(list_menu_count, shop_list_no_count) AS mCVR2
        -- ,  safe_divide(menu_checkout_count , shop_menu_no_count) AS mCVR3
        -- ,  safe_divide(checkout_transaction_count, checkout_no_count) AS mCVR4
        -- , min_date AS min_date
        -- , max_date AS max_date

    from join_users_to_orders

    )


    , test_list_info AS (
        SELECT DISTINCT 
        exper.test_name,
        (CAST(exper.variation_share AS FLOAT64)/100)  AS percentage,
        exper.hypothesis,
        exper.is_active,
        logs.email,
        exper.test_start_date,
        exper.test_end_date
        FROM `fulfillment-dwh-production.cl.dps_experiment_setups` exper
        LEFT JOIN `fulfillment-dwh-production.cl.audit_logs` logs ON exper.test_name = logs.dps.create_experiment.name
        INNER JOIN (
            select distinct
            test_name
            from calculate_avg_metrics
        ) so
            on exper.test_name = so.test_name
        WHERE 1=1
        AND exper.country_code = CtryCode 
        AND variation_group = 'Variation1'
        AND exper.test_name NOT LIKE "%miscon%"
        AND EXTRACT(DATE FROM test_start_date) >= DateIndicator
    )

    , add_test_list_info as (

        select
        o.*,
        tl.* except(test_name)
        from calculate_avg_metrics o
        LEFT JOIN test_list_info tl
            using(test_name)

    )

    SELECT * FROM add_test_list_info;
 

-- Persist Data



    EXECUTE IMMEDIATE (
        '''
        CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Orders_'''|| vertical ||'''`
        AS
        SELECT
        *
        FROM staging_orders
        '''
    );
