CREATE OR REPLACE PROCEDURE `dh-logistics-product-ops.pricing.mena_tb_qdd`(CtryCode STRING, DateIndicator DATE, vertical STRING)
BEGIN

    -- DECLARE CtryCode STRING DEFAULT 'ae';
    -- DECLARE DateIndicator DATE DEFAULT "2022-01-01";
    -- DECLARE vertical STRING DEFAULT "food";


-- Define variable

    DECLARE orders_query STRING;

-- Orders and Sessions 
    IF vertical = "food" THEN
        CREATE OR REPLACE TEMP TABLE pre_staging_orders AS
        SELECT *
        FROM `dh-logistics-product-ops.pricing.qdd_pre_staging_table`
        WHERE created_date_local >= DateIndicator
        AND country_code = CtryCode 
        AND vertical_type = "restaurants"
        ;
    ELSE
        CREATE OR REPLACE TEMP TABLE pre_staging_orders AS
        SELECT *
        FROM `dh-logistics-product-ops.pricing.qdd_pre_staging_table`
        WHERE created_date_local >= DateIndicator
        AND country_code = CtryCode 
        AND vertical_type <> "restaurants";
    END IF;

    ######################################################## ORDERS METRICS ####################################################################################
        CREATE OR REPLACE TEMP TABLE staging_orders AS  
        WITH orders AS (
            SELECT
            test_name
            , variant
            , target_group
            , COUNT(DISTINCT platform_order_code) AS Order_qty
            , COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" then platform_order_code end) as Campaign_Orders
            , COUNT(DISTINCT CASE WHEN dps_delivery_fee_local = 0 then platform_order_code end) as Free_Delivery_Orders
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
            , MAX(created_date) AS created_date

            -- Metric Added on 13-Feb-2023
            , SUM(dps_small_order_fee_local) as dps_small_order_fee_local
            , COUNT(case when is_gfv_below_mov then platform_order_code end) as orders_below_mov

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
            , sum(Free_Delivery_Orders) as Free_Delivery_Orders
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
            , MAX(created_date) AS created_date
            
            -- Metric Added on 13-Feb-2023
            , SUM(dps_small_order_fee_local) as dps_small_order_fee_local
            , SUM(orders_below_mov) as orders_below_mov


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
            , sum(Free_Delivery_Orders) as Free_Delivery_Orders
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
            
            -- Metric Added on 13-Feb-2023
            , SUM(dps_small_order_fee_local) as dps_small_order_fee_local
            , SUM(orders_below_mov) as orders_below_mov


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

    ######################################################## CVR METRICS ####################################################################################

    ############################ BLOCK USING RL LAYER
        --- CURRENTLY UNAVAILABLE UNTIL RL CVR IS FIXED 
        --     , load_cvr as (
        --         SELECT 
        --         test_name
        --         , _level
        --         , variant
        --         , treatment
        --         , target_group
        --         , session_id_count
        --         , users_count_per_test
        --         , transaction_no_count
        --         , list_menu_count
        --         , shop_list_no_count
        --         , shop_menu_no_count
        --         , menu_checkout_count
        --         , checkout_no_count
        --         , checkout_transaction_count
        --         , created_date
        --         FROM `fulfillment-dwh-production.rl.dps_ab_test_dashboard_cvr_v2`
        --         WHERE 1=1
        --         and country_code = CtryCode
        --         AND created_date >= DateIndicator
        --         AND variant NOT IN ('Original')
        --         AND variant IS NOT NULL
                    
        --     )

        --     , user_all_level as (
        --             select 
        --             test_name
        --             , variant
        --             , target_group
        --             , SUM(session_id_count) AS total_sessions
        --             , SUM(transaction_no_count) AS transaction_no_count
        --             , SUM(list_menu_count) AS list_menu_count
        --             , SUM(shop_list_no_count) as shop_list_no_count
        --             , sum(shop_menu_no_count) as shop_menu_no_count
        --             , SUM(menu_checkout_count) AS menu_checkout_count
        --             , sum(checkout_no_count) as checkout_no_count
        --             , SUM(checkout_transaction_count) AS checkout_transaction_count
        --             , MIN(created_date) AS min_date
        --             , MAX(created_date) AS max_date
        --             FROM load_cvr
        --             WHERE target_group = "All"
        --             AND treatment = "All" 
        --             GROUP BY 1,2,3

        --     )

        --     , user_all_treatment as (
        --             select 
        --             test_name
        --             , variant
        --             , "Treatment" as target_group
        --             , SUM(session_id_count) AS total_sessions
        --             , SUM(transaction_no_count) AS transaction_no_count
        --             , SUM(list_menu_count) AS list_menu_count
        --             , SUM(shop_list_no_count) as shop_list_no_count
        --             , sum(shop_menu_no_count) as shop_menu_no_count
        --             , SUM(menu_checkout_count) AS menu_checkout_count
        --             , sum(checkout_no_count) as checkout_no_count
        --             , SUM(checkout_transaction_count) AS checkout_transaction_count
        --             , MIN(created_date) AS min_date
        --             , MAX(created_date) AS max_date
        --             FROM load_cvr
        --             WHERE target_group = "All"
        --             AND treatment = "True" 
        --             GROUP BY 1,2,3
        --     )

        --     , user_target_group as (
        --             select 
        --             test_name
        --             , variant
        --             , target_group
        --             , SUM(session_id_count) AS total_sessions
        --             , SUM(transaction_no_count) AS transaction_no_count
        --             , SUM(list_menu_count) AS list_menu_count
        --             , SUM(shop_list_no_count) as shop_list_no_count
        --             , sum(shop_menu_no_count) as shop_menu_no_count
        --             , SUM(menu_checkout_count) AS menu_checkout_count
        --             , sum(checkout_no_count) as checkout_no_count
        --             , SUM(checkout_transaction_count) AS checkout_transaction_count
        --             , MIN(created_date) AS min_date
        --             , MAX(created_date) AS max_date
        --             FROM load_cvr
        --             WHERE target_group <> "All"
        --             AND treatment = "All" 
        --             GROUP BY 1,2,3
        --     )

        --     , get_distinct_users as (
        --             select 
        --             test_name
        --             , variant
        --             , SUM(users_count_per_test) AS Distinct_users
        --             FROM load_cvr
        --             WHERE target_group = "All"
        --             AND treatment = "All" 
        --             GROUP BY 1,2
        --     )

        -- , union_cvrs as (

        --         SELECT *
        --         FROM user_all_level

        --         UNION ALL 

        --         SELECT *
        --         FROM user_all_treatment

        --         UNION ALL 

        --         SELECT *
        --         FROM user_target_group

        -- )

        --     , add_distinct_users as (

        --             SELECT a.*
        --             , b.Distinct_users
        --             FROM union_cvrs a
        --             LEFT JOIN get_distinct_users b
        --                     USING(test_name, variant)
        --     )

    ########################################################

    ############################ BLOCK USING WORKAROUND 
        , load_keys_per_target_group as (
        ---- THERE'S A 1:1 MAP BETWEEN TARGET GROUPS AND VENDOR_GROUP_ID!
        SELECT DISTINCT 
        entity_id
        , test_id as experiment_id
        , test_name
        , vendor_group_id
        , variation_group as variant
        , CONCAT("Target Group ", priority) as target_group

        FROM `fulfillment-dwh-production.curated_data_shared.dps_experiment_setups`
        where DATE(test_start_date) >= DateIndicator
        -- AND test_name = test_name_filter
        and country_code = CtryCode
        and misconfigured = FALSE
        )

        , add_test_name as (
            SELECT DISTINCT 
            entity_id
            , test_name
            , experiment_id
            FROM load_keys_per_target_group
        )

        , load_cvr as (
            SELECT *
            FROM `fulfillment-dwh-production.curated_data_shared.dps_cvr_events`
            WHERE created_date >= DateIndicator
            AND country_code = CtryCode
            ---- IF experiment is not null --> all sessions relevant to an experiment
            AND experiment_id IS NOT NULL

        )


        , add_tg_definition as (

        SELECT cvr.* EXCEPT(is_in_treatment)
        , tt.test_name
        , test.target_group
            ---- TREATMENT IS ONLY THE TARGET GROUP IS NOT NULL AS VENDOR_GROUP_ID WILL BE
            ---- NO NULL TOO FOR OTHER EXPERIMENTS IN THE CASE OF PARALLEL TESTING
        , IF(test.target_group IS NOT NULL, TRUE, FALSE) as is_in_treatment
        -- , IF(o.platform_order_code IS NOT NULL, TRUE, FALSE) as is_parent_vertical_order
        -- , platform_order_code

        from load_cvr cvr
        LEFT JOIN load_keys_per_target_group test
            ------ We use Vendor_group_id to get the target group
            ON cvr.entity_id = test.entity_id
            AND cvr.experiment_id = test.experiment_id 
            AND cvr.variant = test.variant
            AND cvr.vendor_group_id = test.vendor_group_id
        --- workaround to account only orders in the same parent vertical
        -- LEFT JOIN pre_staging_orders o 
        --     ON cvr.transaction_no = o.ga_session_id
        --     AND cvr.dps_session_id = o.dps_session_id
        INNER JOIN add_test_name tt
            ON cvr.entity_id = tt.entity_id 
            AND cvr.experiment_id = tt.experiment_id

        )

        , tg_metrics as (
        SELECT 
        test_name
        , variant
        , target_group
        , COUNT(DISTINCT ga_session_id) total_sessions
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then transaction_no END) transaction_no_count
        , COUNT(DISTINCT list_menu) list_menu_count
        , COUNT(DISTINCT shop_list_no) shop_list_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then shop_menu_no END) shop_menu_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then menu_checkout END) menu_checkout_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_no END) checkout_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_transaction END) checkout_transaction_count
        , MIN(created_date_local) as min_date
        , MAX(created_date_local) as max_date
        FROM add_tg_definition
        WHERE target_group is not null
        GROUP BY 1,2,3
        )

        , all_level as (
            SELECT
            test_name 
        , variant
        , "All" as target_group
        , COUNT(DISTINCT ga_session_id) total_sessions
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then transaction_no END) transaction_no_count
        , COUNT(DISTINCT list_menu) list_menu_count
        , COUNT(DISTINCT shop_list_no) shop_list_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then shop_menu_no END) shop_menu_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then menu_checkout END) menu_checkout_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_no END) checkout_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_transaction END) checkout_transaction_count
        , MIN(created_date_local) as min_date
        , MAX(created_date_local) as max_date
        FROM add_tg_definition
        GROUP BY 1,2,3
        )

        , treatment_level as (
            SELECT
            test_name 
        , variant
        , "Treatment" as target_group
        , COUNT(DISTINCT ga_session_id) total_sessions
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then transaction_no END) transaction_no_count
        , COUNT(DISTINCT list_menu) list_menu_count
        , COUNT(DISTINCT shop_list_no) shop_list_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then shop_menu_no END) shop_menu_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then menu_checkout END) menu_checkout_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_no END) checkout_no_count
        , COUNT(DISTINCT CASE WHEN vertical_parent_in_test then checkout_transaction END) checkout_transaction_count
        , MIN(created_date_local) as min_date
        , MAX(created_date_local) as max_date
        FROM add_tg_definition
        WHERE target_group is not null
        GROUP BY 1,2,3
        )

        , distinct_users as (
        SELECT 
        test_name
        , variant
        , COUNT(DISTINCT perseus_client_id) as Distinct_users
        FROM add_tg_definition
        GROUP BY 1,2
        )

        , append_cvr_level as (

        SELECT *
        FROM all_level

        UNION ALL

        SELECT *
        FROM treatment_level 

        UNION ALL

        SELECT *
        FROM tg_metrics 
        )

        , add_distinct_users as (
        SELECT
        a.*
        , b.Distinct_users

        FROM append_cvr_level a
        LEFT JOIN distinct_users b
            USING(test_name, variant)
        )
    ######################################################## 

    ############################ JOIN CVR AND ORDERS


        , join_users_to_orders as (
            select
            o.*
            , s.* except(test_name, variant, target_group)
            from join_order_metrics o
            LEFT JOIN  add_distinct_users s
                using(test_name, variant, target_group)

        )

        , calculate_avg_metrics as (
            select
            concat(test_name, variant, target_group) as code
            ,  test_name
            ,  variant
            ,  target_group
            ,  Order_qty as Orders
            ,  Campaign_Orders
            ,  SAFE_DIVIDE(Free_Delivery_Orders, Order_qty) as Free_Delivery_Share
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
            ,  safe_divide(transaction_no_count, shop_menu_no_count) AS CVR3
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
            -- Metris added on 10/Feb/2023
            ,  safe_divide(gfv_local, gfv_eur) AS avg_fx_rate
            -- Metric Added on 13-Feb-2023
            ,  safe_divide(dps_small_order_fee_local, Order_qty) as Avg_DPS_MOV_Fee
            ,  SAFE_DIVIDE(orders_below_mov, Order_qty) as share_orders_below_mov



        from join_users_to_orders

        ) 

    ########################################################
    ############################ ADD SIGNIFCANCE DATA

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
            from qdd_with_signifiance o
            LEFT JOIN test_list_info tl
                using(test_name)

        )
        SELECT * 
        , CURRENT_DATE() AS last_run
        FROM add_test_list_info;
    ########################################################
    
    ############################ PERSIST DATA
        SET orders_query = '''
            CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Orders_'''|| vertical ||'''`
            AS
            SELECT
            *
            FROM staging_orders
            ''';

        EXECUTE IMMEDIATE orders_query;
    ########################################################
END;