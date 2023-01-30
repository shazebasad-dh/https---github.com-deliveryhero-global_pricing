
--Variable setting
DECLARE CtryCode STRING;
DECLARE DateIndicator DATE;
DECLARE vertical STRING;
DECLARE entity_filter STRING;
 
--Definite Inputs
SET CtryCode = 'tr';
SET entity_filter = "YS_TR";
SET DateIndicator = '2022-01-01';
SET vertical = "food";


    CREATE OR REPLACE EXTERNAL TABLE `dh-logistics-product-ops.pricing._tr_vale_vendor_commission_list`
        (
        vendor_id STRING,
        commission_rate FLOAT64
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/12BPyxw-Tfe7ndiHXW87oHsr86ZjVmPbaKbEUBu7LoOI/edit#gid=1206800966"],
        sheet_range="Sheet2!A1:B22408",
        skip_leading_rows=1
    );


    CREATE OR REPLACE TEMP TABLE pre_staging_orders AS  
    WITH 
    
    commission_percentage as (
        select
        lower(vendor_id) as vendor_id
        , safe_divide(commission_rate,100) as commission_percentage

        from `dh-logistics-product-ops.pricing._tr_vale_vendor_commission_list`
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
        map.gfv_local,
        map.gfv_eur,
        CASE
            WHEN map.commission_local IS NULL THEN gfv_local * cm.commission_percentage
            WHEN map.commission_local = 0 THEN gfv_local * cm.commission_percentage
            ELSE map.commission_local 
        END AS commission_local,
        map.delivery_costs_local,
        map.to_customer_time,
        map.travel_time,
        map.delivery_distance,
        map.mean_delay,
        map.created_date,
        country_code,
        map.service_fee_local,
        target_group,
        vendor_price_scheme_type
        FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` map
        LEFT JOIN commission_percentage cm    
        ON map.vendor_id = cm.vendor_id
        LEFT JOIN load_dps_sessions
          using(platform_order_code, country_code)
        WHERE 1=1
        AND country_code = CtryCode
        AND variant NOT IN ('Original') 
        AND variant IS NOT NULL
        AND map.created_date >=  DateIndicator
        AND test_name NOT LIKE "%miscon%"
        AND delivery_distance IS NOT NULL
        AND travel_time IS NOT NULL    
        )

    select
    *
    from real_df;


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
        , sum(Free_Delivery_Orders) Free_Delivery_Orders
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
        , sum(Free_Delivery_Orders) Free_Delivery_Orders
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
        ,  safe_divide(list_menu_count, shop_list_no_count) AS mCVR2
        ,  safe_divide(menu_checkout_count , shop_menu_no_count) AS mCVR3
        ,  safe_divide(checkout_transaction_count, checkout_no_count) AS mCVR4
        , min_date AS min_date
        , max_date AS max_date

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
    FROM add_test_list_info a
    LEFT JOIN significance_data b
      ON a.variant = b.variant_b
      AND a.test_name = b.test_name
      AND a.target_group = b.treatment_1
    )


    SELECT * FROM qdd_with_signifiance;
 





    EXECUTE IMMEDIATE (
        '''
        CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._'''|| upper(CtryCode) ||'''_AB_Results_Orders_'''|| vertical ||'''`
        AS
        SELECT
        *
        FROM staging_orders
        '''
    );

-- END



#################################################### OD SHARE

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.TR_AB_ODShare_Food` AS
with user_in_test as (
  SELECT entity_id
    , test_name
    , perseus_client_id
    , test_start_date
    , test_end_date
    , timestamp_log_minute
    , variant
 FROM `dh-logistics-product-ops.pricing.dps_user_test_clean` 
 LEFT JOIN UNNEST(variant_array) va
 WHERE entity_id = entity_filter
 AND has_multiple_variant = FALSE

)

, load_vendors as (
  SELECT vendor_id 
  FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.vendors`
  WHERE vertical_type = "restaurants"
  AND global_entity_id = entity_filter
)

, load_dps_sessions as (
  SELECT entity_id
  , platform_order_code
  , is_own_delivery
  ,  perseus_client_id
  , order_placed_at
  , vendor_id
 FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
 WHERE created_date >= DateIndicator
 AND entity_id = entity_filter
 AND is_sent
)

, od_share_per_test as (
  SELECT
  CONCAT(test_name, variant) as code
  , test_name
  , variant
  , COUNT(case when is_own_delivery then platform_order_code end) as od_order_count
  , COUNT(platform_order_code) as order_count
  , SAFE_DIVIDE(COUNT(case when is_own_delivery then platform_order_code end), COUNT(platform_order_code)) AS od_share
  FROM load_dps_sessions dps
  INNER JOIN user_in_test user
    ON dps.entity_id = user.entity_id
    AND dps.perseus_client_id = user.perseus_client_id
    AND order_placed_at BETWEEN timestamp_log_minute AND test_end_date
  INNER JOIN load_vendors v
    ON dps.vendor_id = v.vendor_id
  GROUP BY 1,2,3
)

SELECT * FROM od_share_per_test ORDER BY 1,2