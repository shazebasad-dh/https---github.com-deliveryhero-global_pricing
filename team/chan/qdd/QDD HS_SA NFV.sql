 --Variable setting
DECLARE CtryCode STRING;
DECLARE DateIndicator DATE;
DECLARE vertical STRING;


-- Wave inputs
DECLARE test_arrays ARRAY<STRING>;
DECLARE AllTestVar STRING DEFAULT "All Other cities";
DECLARE WaveStart DATE DEFAULT "2023-01-24";
DECLARE riyadh_test DEFAULT "SA_20230124_A_B0_O_Riyadh Other vertical";
DECLARE WaveShare FLOAT64 DEFAULT 0.33;
DECLARE WaveEnd DATE DEFAULT NULL;




 
--Definite Inputs
SET CtryCode = 'sa';
SET DateIndicator = '2023-01-01';
SET test_arrays = [ "SA_20230124_A_B0_O_Riyadh Other vertical"
  , "SA_20230124_A_B0_O_EP Other vertical"
  , "SA_20230124_A_B0_O_Jeddah Other vertical"
  , "SA_20230124_A_B0_O_Madina Other vertical"
  , "SA_20230124_A_B0_O_Jazan Other vertical"
  , "SA_20230124_A_B0_O_Hafar Al Batin Other vertical"
  , "SA_20230124_A_B0_O_Yanbu Other vertical"
  , "SA_20230124_A_B0_O_Abha Other vertical"
  , "SA_20230124_A_B0_O_Alkharj Other vertical"
  , "SA_20230124_A_B0_O_Jubail Other vertical"
  , "SA_20230124_A_B0_O_Hail Other vertical"
  , "SA_20230124_A_B0_O_Ahsa Other vertical"
  , "SA_20230124_A_B0_O_Tabuk Other vertical"
  , "SA_20230124_A_B0_O_QP Other vertical"
  , "SA_20230124_A_B0_O_Mecca Other vertical"
  , "SA_20230124_A_B0_O_Taif Other vertical"
];


    -- CREATE OR REPLACE EXTERNAL TABLE `dh-logistics-product-ops.pricing._sa_rdf_vendors`
    --     (
    --     year STRING,
    --     month FLOAT64,
    --     offer_type STRING,
    --     restaurant_max_charge FLOAT64,
    --     delivery_fee FLOAT64
    --     )
    --     OPTIONS (
    --     format="GOOGLE_SHEETS",
    --     uris=["https://docs.google.com/spreadsheets/d/1-u6bmK6NpeRZyPEGmu4NKCj-ia0S1AWWvDciJG_wtdQ/edit#gid=1359779822"],
    --     sheet_range="data!A1:E7",
    --     skip_leading_rows=1
    -- );


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
        AND vertical_type <> 'restaurants' 
        AND test_name in UNNEST(test_arrays)

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
 
 
  CREATE OR REPLACE TEMP TABLE staging_orders AS  
  with orders_data AS (
        SELECT
        test_name
        , variant
        , target_group
        , COUNT(DISTINCT platform_order_code) AS Orders
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


####### CALCULATE METRICS AT TARGET GROUP LEVEL

    , treatment_level_kpi as (
        select
        test_name
        , variant
        , "Treatment" as target_group
        , sum(Orders) Orders
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

        from orders_data
        where target_group is not null
        group by 1,2,3
    )

    , test_level_metric as (
        select
        test_name
          , variant
          , "All" as target_group
          , sum(Orders) Orders
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
        from orders_data
        group by 1,2,3
    )

    , append_order_metrics as (
        (
            select
            *
            from orders_data
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
            from test_level_metric
        )
    )

########## COMBINE TEST_METRICS

  , orders_wave_metrics as (

  select
   AllTestVar as test_name
    , variant 
    , target_group
    , SUM(Orders) Orders
    , SUM(Campaign_Orders) as Campaign_Orders
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
  from append_order_metrics
  WHERE test_name <> riyadh_test
  Group by 1,2,3

)


, orders_append_wave_metrics as (
  (
    select
    *
    from append_order_metrics
  )

  UNION ALL

  (
    select
    *
    from orders_wave_metrics
  )
)


  ########### SESSION DATA

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
        AND test_name in UNNEST(test_arrays)
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


  ######## USERS WAVE METRICS

        , users_wave_metrics as (
        SELECT 
        AllTestVar
        , variant
        , target_group
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

        from union_user_metrics
        WHERE test_name <> riyadh_test
        group by 1,2,3

    )


        , users_append_wave_metrics as (
          
          (
            select
            *
            from union_user_metrics
          )

          UNION ALL
          (
            SELECT
            *
            FROM users_wave_metrics
          )
  )

      , correct_user_distinct_users as (
        select
        test_name
        , variant 
        , Distinct_users

        from users_append_wave_metrics
        where target_group = "All"

    )

  ######## AVG METRICS

    , join_users_to_orders as (
        select
        o.*
        , s.* except(test_name, variant, target_group, Distinct_users)
        , d.Distinct_users
        from orders_append_wave_metrics o
        LEFT JOIN  users_append_wave_metrics s
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
        ,  Orders
        ,  Campaign_Orders
        ,  RDF_Orders
        ,  safe_divide(delivery_fee_local, Orders) AS Avg_Paid_DF
        ,  safe_divide(dps_delivery_fee_local, Orders) AS Avg_DPS_DF
        ,  safe_divide(dps_surge_fee_local, Orders) AS Avg_Surge_Fee
        ,  safe_divide(dps_minimum_order_value_local, Orders) AS AVG_MOV
        ,  safe_divide(gfv_local, Orders) AS AVG_FV
        ,  safe_divide(gfv_eur, Orders) AS AVG_FV_EUR
        ,  safe_divide(commission_local, Orders) AS AVG_Commission
        ,  safe_divide(delivery_costs_local, Orders) AS AVG_Delivery_Costs
        ,  safe_divide(to_customer_time, Orders) AS AVG_To_Customer_Time
        ,  safe_divide(travel_time, Orders) AS AVG_Travel_Time
        ,  safe_divide(delivery_distance, Orders) AS AVG_Manhattan_Distance
        ,  safe_divide(mean_delay, Orders) AS AVG_Fleet_Delay
        ,  safe_divide(service_fee_local, Orders) as AVG_Service_Fee
        ,  safe_divide(rdf_vendor_contribution, Orders) as AVG_rdf_vendor_contribution
        ,  created_date as max_order_date
        ,  total_sessions
        ,  Distinct_users
        ,  safe_divide(transaction_no_count, total_sessions) AS CVR
        ,  safe_divide(list_menu_count, shop_list_no_count) AS mCVR2
        ,  safe_divide(menu_checkout_count , shop_menu_no_count) AS mCVR3
        ,  safe_divide(checkout_transaction_count, checkout_no_count) AS mCVR4
        ,  min_date AS start_date
        ,  max_date AS end_date
        ,  safe_divide(transaction_no_count , shop_menu_no_count) AS CVR3


    from join_users_to_orders
    )
     
 
  ####################### TEST INFO
  
    , test_list AS (
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
        AND exper.test_name in UNNEST(test_arrays)

    )


    , add_is_all as (
      select
      AllTestVar as test_name
      , WaveShare  
      , CAST(NULL AS STRING)
      , CAST(NULL AS BOOL) 
      , CAST(NULL AS STRING)
      , CAST(WaveStart AS TIMESTAMP)
      , CAST(WaveEnd AS TIMESTAMP)
    )

    , union_test as (
      (
        select
        *
        from test_list
      )

      UNION ALL

      (
        select
        *
        from add_is_all
      )
    )

  ####################### JOIN TEST INFO

    , add_test_list_info as (

          select
          o.*,
          tl.* except(test_name)
          from calculate_avg_metrics o
          LEFT JOIN union_test tl
              using(test_name)

      )

      SELECT * FROM add_test_list_info;


  ############ PERSIST TABLES

      EXECUTE IMMEDIATE (
          '''
          CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Orders_NFV`
          AS
          SELECT
          *
          FROM staging_orders
          '''
      );