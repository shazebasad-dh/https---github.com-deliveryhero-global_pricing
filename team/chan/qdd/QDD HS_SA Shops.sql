######################################################## INPUTS
    /*
    Only use for testing when making changes to the logic
    */
    DECLARE CtryCode STRING DEFAULT 'sa';
    DECLARE DateIndicator DATE DEFAULT "2023-01-01";
    DECLARE vertical STRING DEFAULT "shops";
    DECLARE entity_filter STRING DEFAULT "HS_SA";

########################################################

######################################################## CREATE STAGING TABLE


      CREATE TEMP TABLE staging_table AS
      WITH hs_rdf_orders AS (
          SELECT 
            platform_order_code
            , rdf_offer_applied AS is_rdf_order
            , rdf_offer_restaurant_max_charge AS rdf_vendor_contribution
            , rdf_offer_type
            , delivery_fee_discount
            , subscribed_discount_amount
            , is_subscribed
            , is_user_subscribed
          FROM `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders` 
          WHERE operation_day >= DATE_ADD(DateIndicator, INTERVAL 2 DAY)
        )

        , load_ab_test_orders AS (

          SELECT
            created_date_local
            , test_name
            , test_variant as variant
            , perseus_client_id
            , CAST(platform_order_code AS INT64) platform_order_code
            , vertical_type
            , delivery_fee_local
            , service_fee_local
            , mov_customer_fee_local
            , dps_delivery_fee_local
            , dps_surge_fee_local
            , dps_minimum_order_value_local
            , vendor_price_scheme_type
            , city_name
            , gfv_local
            , gfv_eur
            , gmv_local
            , commission_local
            , delivery_costs_local
            , to_customer_time
            , travel_time
            , delivery_distance
            , mean_delay
            , created_date
            , target_group
            , vendor_id 
          FROM `fulfillment-dwh-production.cl.dps_test_orders` 
          WHERE created_date >= DATE_ADD(DateIndicator, INTERVAL 2 DAY)
            AND entity_id = "HS_SA" 
            AND created_date_local >= DateIndicator
            AND test_variant NOT IN ('Original') 
            AND test_variant IS NOT NULL 
            AND test_name NOT LIKE "%miscon%"
            AND delivery_distance IS NOT NULL
            AND travel_time IS NOT NULL 
            AND is_sent
            AND vertical_type NOT IN ("restaurants", "darkstores")
        )

        , join_sources as (

          SELECT
          ab.* EXCEPT(delivery_fee_local, commission_local)
          , rdf.rdf_offer_type
          , IF(is_rdf_order=1, TRUE, FALSE) AS is_rdf_order
          , IFNULL(rdf_vendor_contribution, 0) AS rdf_vendor_contribution
          , IFNULL(delivery_fee_discount, 0) AS delivery_fee_discount
          , IFNULL(subscribed_discount_amount, 0) AS subscribed_discount_amount
          , IFNULL(is_subscribed, FALSE) AS is_subscribed
          , IFNULL(is_user_subscribed, FALSE) AS is_user_subscribed
          , CASE 
              WHEN rdf.is_rdf_order = 1 THEN delivery_fee_local - rdf.delivery_fee_discount
              ELSE delivery_fee_local
          END as delivery_fee_local
          -- adjust commision according to local team
          , IF(commission_local > 0, gfv_local * 0.22, commission_local) AS commission_local


          FROM load_ab_test_orders ab

          LEFT JOIN hs_rdf_orders rdf
            ON ab.platform_order_code = rdf.platform_order_code
        )

        SELECT *
        , delivery_fee_local + commission_local + rdf_vendor_contribution as revenue_local
        , delivery_fee_local + commission_local + rdf_vendor_contribution - delivery_costs_local as profit_local
        FROM join_sources
        QUALIFY sum(1) over(partition by platform_order_code ORDER BY vendor_price_scheme_type) = 1;

########################################################

######################################################## CREATE CVR TABLE

    CREATE OR REPLACE TEMP TABLE staging_cvr_table AS
    WITH 
        load_sa_vendors AS (
            SELECT 
            vendor_id
            , global_entity_id
            , vertical_type
            FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.vendors`
            WHERE global_entity_id = entity_filter
        )
            
        , load_cvr as (
            SELECT 
                test_name
                , variant
                , target_group
                , created_date_local
                , perseus_client_id
                , fe_session_id as ga_session_id
                , ANY_VALUE(transaction_no) AS transaction_no
                , ANY_VALUE(shop_menu_no) AS shop_menu_no
                , ANY_VALUE(list_menu) AS list_menu
                , ANY_VALUE(shop_list_no) AS shop_list_no
                , ANY_VALUE(menu_checkout) AS menu_checkout
                , ANY_VALUE(checkout_transaction) AS checkout_transaction
                , ANY_VALUE(checkout_no) AS checkout_no
                
                FROM `fulfillment-dwh-production.cl.dps_test_cvr_treatment` cvr
                LEFT JOIN load_sa_vendors lv
                    ON cvr.entity_id = lv.global_entity_id
                    AND cvr.vendor_code = lv.vendor_id
                WHERE created_date >= DateIndicator
                AND entity_id = entity_filter
                AND (
                    CASE
                    WHEN cvr.vendor_code IS NULL THEN TRUE
                    WHEN vertical_type NOT IN ("restaurants", "darkstores") THEN TRUE
                    ELSE FALSE
                    END
                )
                -- AND test_name = "SA_20231207_L_B0_O_LS_Dmart_Riyadh"
                GROUP BY 1,2,3,4,5,6
            )
        
        SELECT *
        FROM load_cvr;


########################################################

######################################################## TEST IN SCOPE

    CREATE TEMP TABLE test_in_scope AS 
    SELECT DISTINCT 
        test_name,
        (CAST(variation_share AS FLOAT64)/100)  AS percentage,
        hypothesis,
        is_active,
        NULL as email,
        test_start_date,
        test_end_date
    FROM `fulfillment-dwh-production.cl.dps_experiment_setups` 
    WHERE TRUE
        AND country_code = CtryCode 
        AND variation_group = 'Control'
        AND misconfigured = FALSE
        AND EXTRACT(DATE FROM test_start_date) >= DateIndicator;


########################################################

######################################################## ORDERS METRICS 

  CREATE OR REPLACE TEMP TABLE staging_orders AS  
        WITH order_all_level AS (
            SELECT
            test_name
            , variant
            , "All" as target_group
            , COUNT(DISTINCT platform_order_code) AS orders
            , COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" then platform_order_code end) as campaign_orders
            , SAFE_DIVIDE(
                 COUNT(DISTINCT CASE WHEN delivery_fee_local = 0 then platform_order_code end)
                , COUNT(DISTINCT platform_order_code)
            ) as free_delivery_share
            , AVG(delivery_fee_local) AS avg_paid_df
            , AVG(dps_delivery_fee_local) AS avg_dps_df
            , AVG(dps_surge_fee_local) AS avg_surge_fee
            , AVG(dps_minimum_order_value_local) AS avg_mov
            , AVG(gfv_local) AS avg_fv
            , AVG(gfv_eur) AS avg_fv_eur
            , AVG(commission_local) AS avg_commission
            , AVG(delivery_costs_local) AS avg_delivery_costs
            , AVG(to_customer_time) AS avg_to_customer_time
            , AVG(travel_time) AS avg_travel_time
            , AVG(delivery_distance) AS avg_manhattan_distance
            , AVG(mean_delay) AS avg_fleet_delay
            , AVG(service_fee_local) as avg_service_fee
            , AVG(revenue_local) as avg_revenue
            , AVG(profit_local) as avg_profit
            , MAX(created_date_local) AS created_date_local
            , AVG(gmv_local) as avg_gmv
            , COUNT(DISTINCT CASE WHEN is_rdf_order then platform_order_code end) as RDF_Orders
            , AVG(rdf_vendor_contribution) as rdf_vendor_contribution


            FROM staging_table
            group by 1,2,3
        )

        , order_treatment_level as (
            SELECT
            test_name
            , variant
            , "Treatment" as target_group
            , COUNT(DISTINCT platform_order_code) AS orders
            , COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" then platform_order_code end) as campaign_orders
            , SAFE_DIVIDE(
                COUNT(DISTINCT CASE WHEN delivery_fee_local = 0 then platform_order_code end)
                , COUNT(DISTINCT platform_order_code)
            ) as free_delivery_share
            , AVG(delivery_fee_local) AS avg_paid_df
            , AVG(dps_delivery_fee_local) AS avg_dps_df
            , AVG(dps_surge_fee_local) AS avg_surge_fee
            , AVG(dps_minimum_order_value_local) AS avg_mov
            , AVG(gfv_local) AS avg_fv
            , AVG(gfv_eur) AS avg_fv_eur
            , AVG(commission_local) AS avg_commission
            , AVG(delivery_costs_local) AS avg_delivery_costs
            , AVG(to_customer_time) AS avg_to_customer_time
            , AVG(travel_time) AS avg_travel_time
            , AVG(delivery_distance) AS avg_manhattan_distance
            , AVG(mean_delay) AS avg_fleet_delay
            , AVG(service_fee_local) as avg_service_fee
            -- , AVG(small_basket_fee) as avg_small_basket_fee
            -- , AVG(dps_small_order_fee_local) as avg_dps_small_basket_fee
            , AVG(revenue_local) as avg_revenue
            , AVG(profit_local) as avg_profit
            , MAX(created_date_local) AS created_date_local
            -- , COUNT(case when is_gfv_below_mov then platform_order_code end) as orders_below_mov
            , AVG(gmv_local) as avg_gmv
            , COUNT(DISTINCT CASE WHEN is_rdf_order then platform_order_code end) as RDF_Orders
            , AVG(rdf_vendor_contribution) as rdf_vendor_contribution

            FROM staging_table
            WHERE target_group IS NOT NULL
            group by 1,2,3
        )

        , order_tg_level as (
            SELECT
                test_name
                , variant
                , target_group
                , COUNT(DISTINCT platform_order_code) AS orders
                , COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" then platform_order_code end) as campaign_orders
                , SAFE_DIVIDE(
                        COUNT(DISTINCT CASE WHEN delivery_fee_local = 0 then platform_order_code end)
                        , COUNT(DISTINCT platform_order_code)
                ) as free_delivery_share
                , AVG(delivery_fee_local) AS avg_paid_df
                , AVG(dps_delivery_fee_local) AS avg_dps_df
                , AVG(dps_surge_fee_local) AS avg_surge_fee
                , AVG(dps_minimum_order_value_local) AS avg_mov
                , AVG(gfv_local) AS avg_fv
                , AVG(gfv_eur) AS avg_fv_eur
                , AVG(commission_local) AS avg_commission
                , AVG(delivery_costs_local) AS avg_delivery_costs
                , AVG(to_customer_time) AS avg_to_customer_time
                , AVG(travel_time) AS avg_travel_time
                , AVG(delivery_distance) AS avg_manhattan_distance
                , AVG(mean_delay) AS avg_fleet_delay
                , AVG(service_fee_local) as avg_service_fee
                -- , AVG(small_basket_fee) as avg_small_basket_fee
                -- , AVG(dps_small_order_fee_local) as avg_dps_small_basket_fee
                , AVG(revenue_local) as avg_revenue
                , AVG(profit_local) as avg_profit
                , MAX(created_date_local) AS created_date_local
                -- , COUNT(case when is_gfv_below_mov then platform_order_code end) as orders_below_mov
                , AVG(gmv_local) as avg_gmv
                , COUNT(DISTINCT CASE WHEN is_rdf_order then platform_order_code end) as RDF_Orders
                , AVG(rdf_vendor_contribution) as rdf_vendor_contribution

            FROM staging_table
            WHERE target_group IS NOT NULL
            group by 1,2,3
        )

        , join_order_metrics as (
            (
                SELECT *
                FROM order_all_level
            )

            UNION ALL

            (
                SELECT *
                FROM order_treatment_level
            )

            UNION ALL

            (
                SELECT *
                FROM order_tg_level
            )
        )

        SELECT *
        , SAFE_DIVIDE(avg_fv, avg_fv_eur) as avg_fx_rate
        FROM join_order_metrics;

########################################################

######################################################## CVR METRICS 

        CREATE OR REPLACE TEMP TABLE staging_cvr AS  
        WITH 
            cvr_tg_level as (
            SELECT 
            test_name
            , variant
            , target_group
            , COUNT(DISTINCT transaction_no ) AS transaction_no_count
            , COUNT(DISTINCT ga_session_id ) AS session_id_count
            , COUNT(DISTINCT shop_menu_no ) AS shop_menu_no_count
            , COUNT(DISTINCT list_menu ) AS list_menu_count
            , COUNT(DISTINCT shop_list_no ) AS shop_list_no_count
            , COUNT(DISTINCT menu_checkout ) AS menu_checkout_count
            , COUNT(DISTINCT checkout_transaction ) AS checkout_transaction_count
            , COUNT(DISTINCT checkout_no ) AS checkout_no_count
            , MIN(created_date_local) as min_date
            , MAX(created_date_local) as max_date
            FROM staging_cvr_table
            WHERE target_group is not null
            GROUP BY 1,2,3
            )

            , cvr_all_level as (
                SELECT
                test_name 
            , variant
            , "All" as target_group
            , COUNT(DISTINCT transaction_no ) AS transaction_no_count
            , COUNT(DISTINCT ga_session_id ) AS session_id_count
            , COUNT(DISTINCT shop_menu_no ) AS shop_menu_no_count
            , COUNT(DISTINCT list_menu ) AS list_menu_count
            , COUNT(DISTINCT shop_list_no ) AS shop_list_no_count
            , COUNT(DISTINCT menu_checkout ) AS menu_checkout_count
            , COUNT(DISTINCT checkout_transaction ) AS checkout_transaction_count
            , COUNT(DISTINCT checkout_no ) AS checkout_no_count
            , MIN(created_date_local) as min_date
            , MAX(created_date_local) as max_date
            FROM load_cvr
            GROUP BY 1,2,3
            )

            , cvr_treatment_level as (
                SELECT
                test_name 
                , variant
                , "Treatment" as target_group
                , COUNT(DISTINCT transaction_no ) AS transaction_no_count
                , COUNT(DISTINCT ga_session_id ) AS session_id_count
                , COUNT(DISTINCT shop_menu_no ) AS shop_menu_no_count
                , COUNT(DISTINCT list_menu ) AS list_menu_count
                , COUNT(DISTINCT shop_list_no ) AS shop_list_no_count
                , COUNT(DISTINCT menu_checkout ) AS menu_checkout_count
                , COUNT(DISTINCT checkout_transaction ) AS checkout_transaction_count
                , COUNT(DISTINCT checkout_no ) AS checkout_no_count
                , MIN(created_date_local) as min_date
                , MAX(created_date_local) as max_date
            FROM load_cvr
            WHERE target_group is not null
            GROUP BY 1,2,3
            )

            , append_cvr_level as (

            SELECT *
            FROM cvr_all_level

            UNION ALL

            SELECT *
            FROM cvr_treatment_level 

            UNION ALL

            SELECT *
            FROM cvr_tg_level 
            )

            , calculate_cvrs as (
                SELECT
                    test_name
                    , variant
                    , target_group
                    ,  session_id_count as total_sessions
                    ,  SAFE_DIVIDE(transaction_no_count, session_id_count) AS CVR
                    ,  SAFE_DIVIDE(transaction_no_count, shop_menu_no_count) AS CVR3
                    ,  SAFE_DIVIDE(list_menu_count, shop_list_no_count) AS mCVR2
                    ,  SAFE_DIVIDE(menu_checkout_count , shop_menu_no_count) AS mCVR3
                    ,  SAFE_DIVIDE(checkout_transaction_count, checkout_no_count) AS mCVR4
                FROM append_cvr_level
            )

            SELECT *
            FROM calculate_cvrs;

######################################################## 

######################################################## PER USER KPIS

    CREATE OR REPLACE TEMP TABLE staging_per_user AS
    WITH 

    cvr_all_level as (
        SELECT
        test_name
        , variant
        , "All" as target_group
        , perseus_client_id
        , COUNT(DISTINCT ga_session_id) as n_sessions
        FROM staging_cvr_table
        group by 1,2,3,4
    )

    , cvr_target_group_level as (
        SELECT
        test_name
        , variant 
        , target_group
        , perseus_client_id
        , COUNT(DISTINCT ga_session_id) as n_sessions
        FROM staging_cvr_table
        WHERE target_group is not null
        group by 1,2,3,4  
    )

    , cvr_treatment_level as (
        SELECT
        test_name
        , variant 
        , "Treatment" as target_group
        , perseus_client_id
        , COUNT(DISTINCT ga_session_id) as n_sessions
        FROM staging_cvr_table
        WHERE target_group is not null
        group by 1,2,3,4  
    )

    , append_cvr_levels as (
        SELECT *
        FROM cvr_all_level

        UNION ALL

        SELECT *
        FROM cvr_target_group_level

        UNION ALL
        
        SELECT *
        FROM cvr_treatment_level
    )


    ########### ORDER

    , load_orders as (
        SELECT 
        test_name
        , variant
        , target_group
        , perseus_client_id
        , platform_order_code
        , delivery_fee_local
        , dps_delivery_fee_local
        , revenue_local
        , profit_local
        , service_fee_local
        -- , small_basket_fee 
        -- , dps_small_order_fee_local
        , gfv_local
        , commission_local
        , gmv_local
        , travel_time
        , delivery_costs_local
        FROM staging_table
    ) 

    , order_all_level as (
        SELECT
        test_name
        , variant
        , "All" as target_group
        , perseus_client_id
        , COUNT(platform_order_code) as n_orders
        , AVG(delivery_fee_local) AS delivery_fee_local
        , SUM(revenue_local) AS revenue_local
        , SUM(profit_local) AS profit_local
        , SUM(service_fee_local) AS service_fee_local
        , SUM(gfv_local) AS gfv_local
        , SUM(commission_local) AS commission_local
        , SUM(gmv_local) AS gmv_local
        , AVG(travel_time) AS travel_time
        , AVG(delivery_costs_local) AS delivery_costs_local
        FROM load_orders
        group by 1,2,3,4
    )

    , order_target_group_level as (
        SELECT
        test_name
        , variant 
        , target_group
        , perseus_client_id
        , COUNT(platform_order_code) as n_orders
        , AVG(delivery_fee_local) AS delivery_fee_local
        , SUM(revenue_local) AS revenue_local
        , SUM(profit_local) AS profit_local
        , SUM(service_fee_local) AS service_fee_local
        , SUM(gfv_local) AS gfv_local
        , SUM(commission_local) AS commission_local
        , SUM(gmv_local) AS gmv_local
        , AVG(travel_time) AS travel_time
        , AVG(delivery_costs_local) AS delivery_costs_local
        FROM load_orders
        WHERE target_group is not null
        group by 1,2,3,4  
    )

    , order_treatment_level as (
        SELECT
        test_name
        , variant 
        , "Treatment" as target_group
        , perseus_client_id
        , COUNT(platform_order_code) as n_orders
        , AVG(delivery_fee_local) AS delivery_fee_local
        , SUM(revenue_local) AS revenue_local
        , SUM(profit_local) AS profit_local
        , SUM(service_fee_local) AS service_fee_local
        , SUM(gfv_local) AS gfv_local
        , SUM(commission_local) AS commission_local
        , SUM(gmv_local) AS gmv_local
        , AVG(travel_time) AS travel_time
        , AVG(delivery_costs_local) AS delivery_costs_local

        FROM load_orders
        WHERE target_group is not null
        group by 1,2,3,4  
    )

    , append_order_levels as (
        SELECT *
        FROM order_all_level
        UNION ALL

        SELECT *
        FROM order_target_group_level

        UNION ALL

        SELECT *
        FROM order_treatment_level
    )

    , join_sessions_and_orders as (
        SELECT cvr.* 
            , IFNULL(n_orders,0) AS n_orders
            , IFNULL(delivery_fee_local,0) AS delivery_fee_local
            , IFNULL(revenue_local,0) AS revenue_local
            , IFNULL(profit_local, 0) as profit_local
            , IFNULL(service_fee_local,0) AS service_fee_local
            , IFNULL(gfv_local,0) AS gfv_local
            , IFNULL(commission_local,0) AS commission_local
            , IFNULL(gmv_local,0) AS gmv_local
            , IFNULL(travel_time,0) AS travel_time
            , IFNULL(delivery_costs_local,0) AS delivery_costs_local
        FROM append_cvr_levels cvr
        LEFT JOIN append_order_levels o
        ON cvr.test_name = o.test_name 
        AND cvr.perseus_client_id = o.perseus_client_id
        AND cvr.variant = o.variant
        AND cvr.target_group = o.target_group
    )

    SELECT 
    test_name
    , variant
    , target_group 
    , COUNT(perseus_client_id) as n_users
    , AVG(n_orders) as orders_per_user
    , AVG(delivery_fee_local) AS delivery_fee_per_user
    , AVG(revenue_local) AS revenue_per_user
    , AVG(profit_local) AS profit_per_user
    , AVG(service_fee_local) AS service_fee_per_user
    -- , AVG(small_basket_fee ) AS small_basket_fee_per_user
    , AVG(gfv_local) AS gfv_per_user
    , AVG(commission_local) AS commission_per_user
    , AVG(gmv_local) AS gmv_per_user
    -- , AVG(dps_small_order_fee_local) AS dps_small_basket_fee_per_user
    , AVG(travel_time) AS travel_time_per_user
    , AVG(delivery_costs_local) AS delivery_costs_per_user
    FROM join_sessions_and_orders
    GROUP BY 1,2,3;
########################################################

######################################################## SIGNIFICANCE

    CREATE OR REPLACE TEMP TABLE staging_significance AS 
    WITH 
    load_analytics_significance AS (
        SELECT 
            test_name, 
            variant_b as variant, 
            treatment as target_group,
            kpi_label, 
            p_value 
        FROM  `logistics-data-storage-staging.long_term_pricing.dps_ab_test_significance_dataset_v2`
        WHERE variant_a = 'Control'
            AND variant_b <> variant_a 
            AND country_code = CtryCode
    )

    , pivot_analytics_significance as (
        SELECT * 
        FROM load_analytics_significance
        PIVOT (SUM (p_value) FOR kpi_label IN ('orders_per_user'
            , 'dps_delivery_fee_local_per_user'
            , 'mov_customer_fee_local_per_user'
            , 'gmv_local_per_user'
            , 'profit_local_per_user'
            , 'travel_time_per_user'
            , 'service_fee_local_per_user'
            , 'commission_local_per_user'
            , 'delivery_costs_local_per_user'
            , 'revenue_local_per_user'
            , 'delivery_distance_per_user'
            ) 
        )
    )
    
    , load_target_group_significance AS (
        SELECT 
            test_name, 
            variant_b as variant, 
            target_group,
            kpi_label, 
            p_value 
        FROM  `logistics-data-storage-staging.long_term_pricing._sl_dps_ab_test_significance_orders_results`
        WHERE variant_a = 'Control'
            AND variant_b <> variant_a 
            AND country_code = CtryCode
            and target_group <> "All"
            AND target_group <> "True"
    )

    , pivot_tg_significance as (
        SELECT * 
        FROM load_target_group_significance
        PIVOT (SUM (p_value) FOR kpi_label IN ('orders_per_user'
        , 'dps_delivery_fee_local_per_user'
        , 'mov_customer_fee_local_per_user'
        , 'gmv_local_per_user'
        , 'profit_local_per_user'
        , 'travel_time_per_user'
        , 'service_fee_local_per_user'
        , 'commission_local_per_user'
        , 'delivery_costs_local_per_user'
        , 'revenue_local_per_user'
        , 'delivery_distance_per_user'
            ) 
        )
    )

    , append_significances as (
        SELECT *
        FROM pivot_analytics_significance

        UNION ALL 

        SELECT *
        FROM pivot_tg_significance
    )

    SELECT * EXCEPT(
            target_group
        , orders_per_user
        , dps_delivery_fee_local_per_user
        , mov_customer_fee_local_per_user
        , gmv_local_per_user
        , profit_local_per_user
        , travel_time_per_user
        , service_fee_local_per_user
        , commission_local_per_user
        , delivery_costs_local_per_user
        , revenue_local_per_user
        , delivery_distance_per_user
    )
        , IF(target_group = "True", "Treatment", target_group) as target_group
        , orders_per_user AS orders_per_user_p_value
        , dps_delivery_fee_local_per_user AS dps_delivery_fee_local_per_user_p_value
        , mov_customer_fee_local_per_user AS mov_customerfee_local_per_user_p_value
        , gmv_local_per_user AS gmv_local_per_user_p_value
        , profit_local_per_user AS profit_local_per_user_p_value
        , travel_time_per_user AS travel_time_per_user_p_value
        , service_fee_local_per_user AS service_fee_local_per_user_p_value
        , commission_local_per_user AS commission_local_per_user_p_value
        , delivery_costs_local_per_user AS delivery_costs_local_pr_user_p_value
        , revenue_local_per_user AS revenue_local_per_user_p_value
        , delivery_distance_per_user AS delivery_distance_per_user_p_value
    FROM append_significances;
########################################################

######################################################## JOIN EVERYTHING

        CREATE OR REPLACE TEMP TABLE staging_qdd AS
        SELECT
            CONCAT(test_name, variant, target_group) as code
            , o.*
            , s.* except(test_name, variant, target_group)
            , u.* except(test_name, variant, target_group)
            , sg.* except(test_name, variant, target_group)
            , t.* EXCEPT(test_name)
            , CURRENT_DATE() AS last_run

        FROM staging_orders o
        LEFT JOIN staging_cvr s
            using(test_name, variant, target_group)
        LEFT JOIN staging_per_user u
            using(test_name, variant, target_group)
        LEFT JOIN staging_significance sg
            using(test_name, variant, target_group)
        INNER JOIN test_in_scope t
            USING(test_name)
        ;
########################################################
    
######################################################## PERSIST DATA
  
    CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing._SA_AB_Results_Orders_shops`
    AS
    SELECT
    *
    FROM staging_qdd

########################################################