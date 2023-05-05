STAGING_QUERY = """

DECLARE start_date, end_date DATE;

SET end_date = "{0}";
SET start_date = "{1}";

SELECT 
    order_id as platform_order_code
    , operation_day
    , order_created_at_sa
    , branch_id
    , branch_name_en
    , OD_delivery_fee
    , is_acquisition
    , rdf_offer_applied
    , rdf_offer_restaurant_max_charge
    , rdf_offer_type
    , is_subscribed
    , is_user_subscribed
    , delivery_fee_discount
    , subscribed_discount_amount

FROM `dhub-hungerstation.reporting_prod.orders_fact_non_pii` 
WHERE operation_day BETWEEN start_date AND end_date
AND rdf_offer_applied = 1;
"""



MERGE_QUERY = """
MERGE INTO `dh-logistics-product-ops.pricing.{0}` prd
  USING  `dh-logistics-product-ops.pricing.{1}` stg
    ON prd.platform_order_code = stg.platform_order_code
  WHEN MATCHED THEN
    UPDATE SET
        platform_order_code = stg.platform_order_code
        , operation_day = stg.operation_day
        , order_created_at_sa = stg.order_created_at_sa
        , branch_id = stg.branch_id
        , branch_name_en = stg.branch_name_en
        , OD_delivery_fee = stg.OD_delivery_fee
        , rdf_offer_applied = stg.rdf_offer_applied
        , rdf_offer_restaurant_max_charge = stg.rdf_offer_restaurant_max_charge
        , rdf_offer_type = stg.rdf_offer_type
        , is_subscribed = stg.is_subscribed
        , is_user_subscribed = stg.is_user_subscribed
        , delivery_fee_discount = stg.delivery_fee_discount
        , subscribed_discount_amount = stg.subscribed_discount_amount
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
"""

QDD_STAGING_TABLE = """
"""


QDD_QUERY = """
########################################## PARAMS

  DECLARE start_date_filter, end_date_filter DATE;
  DECLARE backfill BOOL;
  DECLARE DateIndicator DATE;

  SET backfill = TRUE;
  /*
  DateIndicator still required as CVR is not an incremental table
  */
  SET DateIndicator = "2022-01-01";

  # SET END DATE 
  SET end_date_filter = CURRENT_DATE();


  # SET PARTITION DATE
  IF backfill THEN 
      SET start_date_filter = DATE_SUB("2022-01-01", interval 0 DAY); 
  ELSE
      SET start_date_filter = DATE_SUB(end_date_filter, interval 30 DAY);
  END IF; 

##########################################

########################################## STAGING TABLE
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
      FROM `dh-logistics-product-ops.pricing.hs_sa_rdf_orders` 
      WHERE operation_day BETWEEN start_date_filter AND end_date_filter
    )

    , average_commission AS (
            SELECT
              vendor_id
              , AVG(gfv_local) avg_gfv
              , AVG(commission_local) avg_commission
            FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
            WHERE created_date BETWEEN CURRENT_DATE()-84 AND CURRENT_DATE()-2
            AND entity_id = "HS_SA"
            AND commission_local <> 0
            AND gfv_local <> 0
            GROUP BY 1
    )
        
    , commission_percentage AS (
            SELECT
              vendor_id
              , SAFE_DIVIDE(avg_commission, avg_gfv) commission_percentage
            FROM average_commission
    )

    , load_dps_sessions AS (
        SELECT
          CAST(platform_order_code AS INT64) platform_order_code
          , country_code
          , vendor_price_scheme_type
          , linear_dist_customer_vendor
        FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
        WHERE created_date BETWEEN start_date_filter AND end_date_filter
        AND entity_id = "HS_SA"
    )

    , load_ab_test_orders AS (

      SELECT
        created_date_local
        , test_name
        , variant
        , CAST(platform_order_code AS INT64) platform_order_code
        , vertical_type
        , delivery_fee_local
        , service_fee_local
        , mov_customer_fee_local
        , dps_delivery_fee_local
        , dps_surge_fee_local
        , dps_minimum_order_value_local
        -- , dps_small_order_fee_local
        , gfv_local
        , gfv_eur
        , commission_local
        , delivery_costs_local
        , to_customer_time
        , travel_time
        , delivery_distance
        , mean_delay
        , created_date
        , target_group
        , vendor_id 
      FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` 
      WHERE created_date BETWEEN start_date_filter AND end_date_filter
      AND entity_id = "HS_SA" 
      AND variant NOT IN ('Original') 
      AND variant IS NOT NULL 
      AND test_name NOT LIKE "%miscon%"
      AND delivery_distance IS NOT NULL
      AND travel_time IS NOT NULL 
      AND is_sent

    )

    , join_sources as (

      SELECT DISTINCT
      ab.* EXCEPT(delivery_fee_local, commission_local)
      , dps.* EXCEPT(platform_order_code)
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
      , CASE
            WHEN ab.commission_local IS NULL THEN gfv_local * cm.commission_percentage
            WHEN ab.commission_local = 0 THEN gfv_local * cm.commission_percentage
            ELSE ab.commission_local 
      END AS commission_local


      FROM load_ab_test_orders ab

      LEFT JOIN load_dps_sessions dps
        ON ab.platform_order_code = dps.platform_order_code

      LEFT JOIN hs_rdf_orders rdf
        ON ab.platform_order_code = rdf.platform_order_code

      LEFT JOIN commission_percentage cm
        ON ab.vendor_id = cm.vendor_id    
    )

    SELECT *
    FROM join_sources;

##########################################

########################################## UPSERT PRE-STAGING TABLE

  IF backfill THEN 
    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._sa_qdd_pre_staging_table`
    PARTITION BY created_date_local
    CLUSTER BY test_name
    AS
    SELECT * FROM staging_table;
  ELSE
    MERGE INTO `dh-logistics-product-ops.pricing._sa_qdd_pre_staging_table` qdd
    USING staging_table stg
      ON qdd.platform_order_code = stg.platform_order_code
      AND qdd.test_name = stg.test_name
    WHEN MATCHED THEN
      UPDATE SET
          created_date_local = stg.created_date_local
          , test_name = stg.test_name
          , variant = stg.variant
          , platform_order_code = stg.platform_order_code
          , vertical_type = stg.vertical_type
          , service_fee_local = stg.service_fee_local
          , mov_customer_fee_local = stg.mov_customer_fee_local
          , dps_delivery_fee_local = stg.dps_delivery_fee_local
          , dps_surge_fee_local = stg.dps_surge_fee_local
          , dps_minimum_order_value_local = stg.dps_minimum_order_value_local
          -- , dps_small_order_fee_local = stg.dps_small_order_fee_local
          , gfv_local = stg.gfv_local
          , gfv_eur = stg.gfv_eur
          , delivery_costs_local = stg.delivery_costs_local
          , to_customer_time = stg.to_customer_time
          , travel_time = stg.travel_time
          , delivery_distance = stg.delivery_distance
          , mean_delay = stg.mean_delay
          , created_date = stg.created_date
          , target_group = stg.target_group
          , vendor_id = stg.vendor_id
          , country_code = stg.country_code
          , vendor_price_scheme_type = stg.vendor_price_scheme_type
          , linear_dist_customer_vendor = stg.linear_dist_customer_vendor
          , rdf_offer_type = stg.rdf_offer_type
          , delivery_fee_discount = stg.delivery_fee_discount
          , subscribed_discount_amount = stg.subscribed_discount_amount
          , is_subscribed = stg.is_subscribed
          , is_user_subscribed = stg.is_user_subscribed
          , delivery_fee_local = stg.delivery_fee_local
          , commission_local = stg.commission_local
          , is_rdf_order = stg.is_rdf_order
          , rdf_vendor_contribution = stg.rdf_vendor_contribution


    WHEN NOT MATCHED THEN
      INSERT ROW
    ;
  end if;


########################################## QDD LOGIC

    CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._SA_AB_Results_Orders_food` AS 

    ########################################## ORDER LOGIC 
      
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
          FROM `dh-logistics-product-ops.pricing._sa_qdd_pre_staging_table`
          where vertical_type = "restaurants"
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
      , all_level_metric AS (
          SELECT
            test_name
            , variant
            , "All" AS target_group
            , SUM(Order_qty) Order_qty
            , SUM(Campaign_Orders) Campaign_Orders
            , SUM(RDF_Orders) AS RDF_Orders
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
            , SUM(service_fee_local) AS service_fee_local
            , sum(rdf_vendor_contribution) AS rdf_vendor_contribution
            , MAX(created_date) AS created_date
          FROM orders
          GROUP BY 1,2,3
      )

      , join_order_metrics AS (
          (
            SELECT *
            FROM orders
            WHERE target_group IS NOT NULL 
          )
          UNION ALL
          (
            SELECT *
            FROM treatment_level_kpi
          )
          UNION ALL
          (
            SELECT *
            FROM all_level_metric
          )
      )

    ##########################################

    ########################################## CVR
      /*
      TODO -> check if new CVR pipeline yields acceptable CVR values
      As of April 2023, we use ga_events_logic.

      TODO -> Transform CVR logic into incremental to speed up
      query
      */
      , session_test_data as (
          select distinct
          test_id as experiment_id
          , test_name
          , entity_id as global_entity_id
          , test_start_date
          , IFNULL(test_end_date, CURRENT_TIMESTAMP()) as test_end_date
          from `fulfillment-dwh-production.cl.dps_experiment_setups`
          where entity_id = "HS_SA"
          and date(test_start_date) >= date_add(DateIndicator, interval 8 month)
      )

      , unique_list_of_users AS (

          SELECT DISTINCT
          test_name
          , s.global_entity_id
          , DATE(test_start_date) AS test_start_date
          , DATE(test_end_date) AS test_end_date
          , fullvisitor_id
          , sessions.experiment_id
          , sessions.variant
          , sessions.perseus_client_id

          FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` ga
          INNER JOIN session_test_data s
            ON ga.sessions.experiment_id = s.experiment_id
            AND  ga.entity_id = s.global_entity_id
          where TRUE
          AND sessions.variant <> "Original"
          AND sessions.variant IS NOT NULL
          AND country_code = "sa"
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

      , users AS (
          SELECT 
          test_name
          , variant
          , target_group
          , SUM(users_count_per_test) AS Distinct_users
          FROM `fulfillment-dwh-production.rl.dps_ab_test_dashboard_cvr_v2`
          WHERE 1=1
          AND country_code = "sa"
          AND variant NOT IN ('Original')
          AND variant IS NOT NULL
          AND target_group IS NOT NULL 
          AND test_name NOT LIKE "%iscon%"
          AND created_date >= DateIndicator
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

    ##########################################

    ########################################## AGGREGATE METRICS

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
      from join_users_to_orders

      )

    ##########################################

    ########################################## METADATA / SIGNIFICANCE
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
          AND exper.country_code = "sa" 
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

      ,significance_data as (

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
      treatment,"All" as zone_name, kpi_label, 
      p_value 
      FROM  `fulfillment-dwh-production.rl.dps_ab_test_significance_dataset_v2`
      WHERE variant_a = 'Control'
      AND country_code = "sa"
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
        -- AND a.zone_name = b.zone_name
      )

    ##########################################
 
    SELECT * 
    , CURRENT_DATE() AS last_run
    FROM qdd_with_signifiance;

##########################################

########################################## RAMADAN 2023 TOD TESTS


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._SA_AB_Results_Orders_food_tod`
  AS
  with  reverse_ab_significance as (
    SELECT 
    test_name
    ,   variant_b
    ,   treatment
    ,   orders_per_user as orders_per_user_tod
    ,   fleet_delay as fleet_delay_tod
    ,   orders_per_customer as orders_per_customer_tod
    ,   travel_time as travel_time_tod
    ,   revenue_local as revenue_local_tod
    ,   gfv_eur as gfv_eur_tod
    ,   gfv_local as gfv_local_tod
    ,   revenue_eur as revenue_eur_tod
    ,   delivery_costs_local as delivery_costs_local_tod
    ,   profit_eur as profit_eur_tod
    ,   delivery_costs_eur as delivery_costs_eur_tod
    ,   profit_local as profit_local_tod
    ,   delivery_fee_local as delivery_fee_local_tod
    ,   delivery_fee_eur as delivery_fee_eur_tod
    ,   delivery_distance as delivery_distance_tod
    ,   mcvr4 as mcvr4_tod
    ,   mcvr3 as mcvr3_tod
    ,   mcvr2 as mcvr2_tod
    ,   user_cvr as user_cvr_tod
    ,   cvr as cvr_tod
    ,   CASE 
            WHEN treatment = 'True' THEN 'Treatment' 
            WHEN treatment = 'All' THEN 'All' 
        END treatment_1_tod
    FROM (
        SELECT test_name, variant_b, treatment,kpi_label, p_value 
        FROM `fulfillment-dwh-production.rl.dps_ab_test_significance_dataset_v2`
        WHERE variant_a = 'Variation1'
        AND variant_b = "Variation2"
    )
    PIVOT (SUM (p_value) FOR kpi_label in ('orders_per_user'
            ,'fleet_delay'
            ,'orders_per_customer'
            ,'travel_time'
            ,'revenue_local'
            ,'gfv_eur'
            ,'gfv_local'
            ,'revenue_eur'
            ,'delivery_costs_local'
            ,'profit_eur'
            ,'delivery_costs_eur'
            ,'profit_local'
            ,'delivery_fee_local'
            ,'delivery_fee_eur'
            ,'delivery_distance'
            ,'mcvr4'
            ,'mcvr3'
            ,'mcvr2'
            ,'user_cvr'
            ,'cvr'
            ) 
        )
    )

    , qdd_with_tod_significance as (
        SELECT a.*
            , b.* EXCEPT(test_name, variant_b, treatment_1_tod) 
        FROM `dh-logistics-product-ops.pricing._SA_AB_Results_Orders_food` a
        LEFT JOIN reverse_ab_significance b
            ON a.variant = b.variant_b
            AND a.test_name = b.test_name
            AND a.target_group = b.treatment_1_tod
        WHERE a.test_name in (
            "SA_20230322_R_J0_O_ Yanbu Ramadan TOD"
            , "SA_20230322_R_J0_O_ Taif Ramadan TOD"
            , "SA_20230322_R_J0_O_ Tabuk Ramadan TOD"
            , "SA_20230322_R_J0_O_ T3 G1 Ramadan TOD"
            , "SA_20230322_R_J0_O_ T3 Ramadan TOD"
            , "SA_20230322_R_J0_O_ Riyadh Ramadan TOD"
            , "SA_20230322_R_J0_O_ QP Ramadan TOD"
            , "SA_20230322_R_J0_O_ Mecca Ramadan TOD"
            , "SA_20230322_R_J0_O_ Madinah Ramadan TOD"
            , "SA_20230322_R_J0_O_ Jeddah Ramadan TOD"
            , "SA_20230322_R_J0_O_ Jazan Ramadan TOD"
            , "SA_20230322_R_J0_O_ Hail Ramadan TOD"
            , "SA_20230322_R_J0_O_ Hafar Al Batin Ramadan TOD"
            , "SA_20230322_R_J0_O_ EP Ramadan TOD -2"
            , "SA_20230322_R_J0_O_ Al Kharj Ramadan TOD"
            , "SA_20230322_R_J0_O_ Al Jubail Ramadan TOD"
            , "SA_20230322_R_J0_O_ Ahsa Ramadan TOD"
            , "SA_20230322_R_J0_O_ Abha Ramadan TOD"
            , "SA_20230405_R_J0_O_ Ahsa Ramadan TOD"
            , "SA_20230405_R_J0_O_ Al Kharj Ramadan TOD"
            , "SA_20230405_R_J0_O_ Jazan Ramadan TOD"
            , "SA_20230405_R_J0_O_ QP Ramadan TOD"
            , "SA_20230405_R_J0_O_ Tabuk Ramadan TOD"
        )
    )

    SELECT *
    FROM qdd_with_tod_significance;
"""