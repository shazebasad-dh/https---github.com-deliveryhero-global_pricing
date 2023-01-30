CREATE OR REPLACE PROCEDURE `dh-logistics-product-ops.pricing.mena_tb_qdd`(CtryCode STRING, DateIndicator DATE, vertical STRING)
BEGIN

    -- DECLARE CtryCode STRING DEFAULT 'ae';
    -- DECLARE DateIndicator DATE DEFAULT "2022-01-01";
    -- DECLARE vertical STRING DEFAULT "food";


-- Define variable

    DECLARE orders_query STRING;
    DECLARE test_list_query STRING;

-- Orders and Sessions 

    
    CREATE OR REPLACE TEMP TABLE pre_staging_orders AS  
    WITH 
    
    average_commission AS(
        SELECT
        vendor_id,
        avg(gfv_local) avg_gfv,
        avg(commission_local) avg_commission
        FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
        WHERE 1=1
        AND country_code = CtryCode
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
        safe_divide(avg_commission, avg_gfv) commission_percentage
        FROM average_commission
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
        map.test_name, 
        map.variant,
        map.order_id,
        vertical_type,
        platform_order_code,
        delivery_fee_local,
        map.dps_delivery_fee_local,
        map.dps_surge_fee_local,
        map.dps_minimum_order_value_local,
        CASE
            WHEN map.commission_local IS NULL THEN gfv_local * cm.commission_percentage
            WHEN map.commission_local = 0 THEN gfv_local * cm.commission_percentage
            ELSE map.commission_local 
        END AS commission_local,
        map.mov_customer_fee_local,
        map.gfv_local,
        map.gfv_eur,
        map.delivery_costs_local,
        map.to_customer_time,
        map.travel_time,
        map.delivery_distance,
        map.mean_delay,
        map.created_date,
        map.country_code,
        map.service_fee_local,
        target_group,
        vendor_price_scheme_type,
        vat_ratio
        FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` map
        LEFT JOIN commission_percentage cm    
        ON map.vendor_id = cm.vendor_id
        LEFT JOIN load_dps_sessions
          using(platform_order_code, country_code)
        WHERE 1=1
        AND map.country_code = CtryCode
        AND variant NOT IN ('Original') 
        AND variant IS NOT NULL
        AND map.created_date >=  DateIndicator
        AND test_name NOT LIKE "%miscon%"
        AND delivery_distance IS NOT NULL
        AND travel_time IS NOT NULL    
        )

        , add_revenue as (
          -- Added in Jan 2023
          SELECT *
          , ifnull(commission_local,0) 
            + ifnull(mov_customer_fee_local, 0)
            + ifnull(delivery_fee_local, 0)
            + ifnull(service_fee_local,0)
          AS revenue_local

          , SAFE_DIVIDE(
            ifnull(commission_local,0) 
            + ifnull(mov_customer_fee_local, 0)
            + ifnull(delivery_fee_local, 0)
            + ifnull(service_fee_local,0)
            , (1+vat_ratio)
          ) as revenue_no_vat_local

          , SAFE_DIVIDE(delivery_fee_local , (1+vat_ratio)) AS df_no_vat_local
          , SAFE_DIVIDE(commission_local, (1+vat_ratio)) as commission_no_vat_local
          -- , SAFE_DIVIDE(service_fee_local, (1+vat_ratio)) as service_fee_no_vat_local
          -- , SAFE_DIVIDE(mov_customer_fee_local, (1+vat_ratio)) as mov_customer_fee_no_vat_local

          FROM real_df
        )

    select
    *
    from add_revenue;


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
        -- Metrics Added on Jan'23
        , SUM(mov_customer_fee_local) as mov_customer_fee_local
        , SUM(revenue_no_vat_local) as revenue_no_vat_local
        , SUM(revenue_local) as revenue_local
        , SUM(df_no_vat_local) as df_no_vat_local
        , SUM(commission_no_vat_local) as commission_no_vat_local
        -- , SUM(service_fee_no_vat_local) as service_fee_no_vat_local
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
         -- Metrics Added on Jan'23
        , SUM(mov_customer_fee_local) as mov_customer_fee_local
        , SUM(revenue_no_vat_local) as revenue_no_vat_local
        , SUM(revenue_local) as revenue_local
        , SUM(df_no_vat_local) as df_no_vat_local
        , SUM(commission_no_vat_local) as commission_no_vat_local
        -- , SUM(service_fee_no_vat_local) as service_fee_no_vat_local
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
        -- Metrics Added on Jan'23
        , SUM(mov_customer_fee_local) as mov_customer_fee_local
        , SUM(revenue_no_vat_local) as revenue_no_vat_local
        , SUM(revenue_local) as revenue_local
        , SUM(df_no_vat_local) as df_no_vat_local
        , SUM(commission_no_vat_local) as commission_no_vat_local
        -- , SUM(service_fee_no_vat_local) as service_fee_no_vat_local
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

    , users_treatment_level as (


        SELECT 
        test_name
        , variant
        , "Treatment" as target_group
        , SUM(total_sessions) AS total_sessions
        , SUM(Distinct_users) AS Distinct_users
        , SUM(transaction_no_count) AS transaction_no_count
        , SUM(list_menu_count) AS list_menu_count
        , SUM(shop_list_no_count) as shop_list_no_count
        , sum(shop_menu_no_count) as shop_menu_no_count
        , SUM(menu_checkout_count) AS menu_checkout_count
        , sum(checkout_no_count) as checkout_no_count
        , SUM(checkout_transaction_count) AS checkout_transaction_count
        , MIN(min_date) AS min_date
        , MAX(max_date) AS max_date

        from users
        where target_group <> "All"
        GROUP BY 1,2,3
    )

    , correct_user_distinct_users as (
        select
        test_name
        , variant 
        , Distinct_users

        from users
        where target_group = "All"

    )

    , union_user_metrics as (
        (
            select
            *
            from users
        )

        UNION ALL
        (
            select
            *
            from users_treatment_level
        )
    )

    , join_users_to_orders as (
        select
        o.*
        , s.* except(test_name, variant, target_group, Distinct_users)
        , d.Distinct_users
        from join_order_metrics o
        LEFT JOIN  union_user_metrics s
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
        ,  created_date as max_order_date
        ,  total_sessions
        ,  Distinct_users
        ,  safe_divide(transaction_no_count, total_sessions) AS CVR
        ,  safe_divide(list_menu_count, shop_list_no_count) AS mCVR2
        ,  safe_divide(menu_checkout_count , shop_menu_no_count) AS mCVR3
        ,  safe_divide(checkout_transaction_count, checkout_no_count) AS mCVR4
        , min_date AS min_date
        , max_date AS max_date
        
        -- Metrics added on Jan'23
        ,  safe_divide(mov_customer_fee_local, Order_qty) AS Avg_MOV_Fee
        ,  safe_divide(df_no_vat_local, Order_qty) AS Avg_DF_No_VAT
        ,  safe_divide(revenue_local, Order_qty) AS Avg_Revenue
        ,  safe_divide(revenue_no_vat_local, Order_qty) AS Avg_Revenue_No_VAT
        ,  safe_divide(commission_no_vat_local, Order_qty) AS Avg_Commission_No_VAT
        -- ,  safe_divide(service_fee_no_vat_local, Order_qty) AS AVG_Service_Fee_No_VAT
        -- ,  safe_divide(mov_customer_fee_local, Order_qty) AS Avg_MOV_Fee_No_VAT


    from join_users_to_orders

    ) 

    , significance_data as (

    SELECT 
    * 
    , case 
      when treatment = 'True' then 'Treatment' 
      when treatment = 'All' then 'All'
    END treatment_1 
    
    FROM (
    SELECT 
    test_name, 
    variant_b, 
    treatment,kpi_label, 
    p_value 
    FROM  `fulfillment-dwh-production.rl.dps_ab_test_significance_dataset_v2`
    WHERE variant_a = 'Control'
    AND country_code = CtryCode
    AND variant_b <> variant_a )
    PIVOT (SUM (p_value) FOR kpi_label in ('orders_per_user'
    ,'fleet_delay'
    ,'travel_time'
    ,'revenue_local'
    ,'gfv_local'
    ,'delivery_costs_local'
    ,'profit_local'
    ,'delivery_fee_local'
    ,'delivery_distance'
    -- adding non delivery fee fees significance
    ,'service_fee_local'
    ,'mov_customer_fee_local'
    ,'commission_local'
    ,'mcvr4'
    ,'mcvr3'
    ,'mcvr2'
    ,'cvr'
        ) 
      )
    )

    , qdd_with_signifiance as (

    SELECT 
    a.*
    , orders_per_user as Orders_p_value
    , fleet_delay as avg_fleet_delay_p_value 
    , travel_time as avg_travel_time_p_value 
    , revenue_local as avg_revenue_p_value 
    , gfv_local as avg_fv_p_value 
    , delivery_costs_local as avg_delivery_costs_p_value 
    , profit_local as avg_profit_p_value 
    , delivery_fee_local as avg_delivery_fee_p_value 
    , delivery_distance as avg_delivery_distance_p_value
    -- adding non delivery fee fees significance
    , service_fee_local as avg_service_fee_local_p_value
    , mov_customer_fee_local as avg_mov_customer_fee_local_p_value
    , commission_local as avg_commission_local_p_value
    , b.mcvr4 as mcvr4_p_value 
    , b.mcvr3 as mcvr3_p_value 
    , b.mcvr2 as mcvr2_p_value 
    , b.cvr as cvr_p_value 
    FROM calculate_avg_metrics a
    LEFT JOIN significance_data b
      ON a.variant = b.variant_b
      AND a.test_name = b.test_name
      AND a.target_group = b.treatment_1
    )


    SELECT * FROM qdd_with_signifiance;
 
-- Test list 
    CREATE OR REPLACE TEMP TABLE staging_test_list AS 
    WITH test_list AS (
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
            from staging_orders
        ) so
            on exper.test_name = so.test_name
        WHERE 1=1
        AND exper.country_code = CtryCode 
        AND variation_group = 'Variation1'
        AND exper.test_name NOT LIKE "%miscon%"
        AND EXTRACT(DATE FROM test_start_date) >= DateIndicator
    )
    SELECT * FROM test_list;

-- Persist Data

    SET orders_query = '''
        CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Orders_'''|| vertical ||'''`
        AS
        SELECT
        *
        FROM staging_orders
        ''';


    SET test_list_query = '''
        CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Test_List_'''|| vertical ||'''`
        AS
        SELECT
        *
        FROM staging_test_list
        ORDER BY test_start_date DESC
        ''';


    EXECUTE IMMEDIATE orders_query;
    EXECUTE IMMEDIATE test_list_query;

END;