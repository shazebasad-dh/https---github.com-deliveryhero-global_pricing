
-----------------------------------------------------------------------------------------------------------------------
-- Name            : pricing_compliance_score.sql
-- Initial Author  : Sebastian Lafaurie
-- Owner           : Global Pricing Team
-- Initial Create  : 2023-10-17 | Sebasitan Lafaurie
-- Additional Notes: This tables containt the full pipeline-DAG to calculate the Pricing Compliance Score.
-- UPDATED         : 

/*
To understand the compliance score see https://docs.google.com/presentation/d/1GFVzpEOAcCFPwSMT0awXeKSATqCLsZwO43PPM0RIR-Y/edit#slide=id.p
file with dimension https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=242936969

The Compliance Score is a ratio of the mechanisms in use in a given market with respect to the ones they could use given their context which
includes quarterly performance metrics and competitive landscape. 

The mechanism in use data (numerator) comes from the work done in the Pricing Mechanism ID.
The mechanism to be used (denominator) mostly relies on a series of manual inputs in GSheets (file with dimension link). Such information
is expected to be updated on a quarterly basis.  

Future updates should target the migration from GSheets towards SQL codes to increase robustness. 
*/
-----------------------------------------------------------------------------------------------------------------------


# compliance score date filter
DECLARE filter_period DATE DEFAULT "2023-07-01";

################################### QUARTERLY PERFORMANCE INPUTS

  # date filters for quarterly performance aggregations
  DECLARE from_date_filter, to_date_filter DATE;
  SET from_date_filter = "2023-03-01";
  SET to_date_filter = "2024-09-01";

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compscore_performance_inputs`
  AS

  ############################################ TRANSACTIONAL DATA WITH SUBS
    WITH countries as (
    select distinct
    segment as region
    , management_entity
    , country_name
    , lower(country_code) as country_code
    , global_entity_id as entity_id
    from `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities`
    )


    , load_cdwh as (
    SELECT
    order_id as platform_order_code
    , DATE(placed_at_local) as created_date_local
    , global_entity_id as entity_id
    , IFNULL(value.service_fee_eur,0) AS service_fee_eur
    , IFNULL(value.delivery_fee_eur,0) AS delivery_fee_eur
    , IFNULL(value.mov_customer_fee_eur,0) AS small_basket_fee_eur
    , value.gbv_eur as gfv_eur
    , value.gmv_eur
    , value.mov_eur
    , value.commission_eur
    , is_qcommerce

    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders`
    WHERE placed_at_local BETWEEN from_date_filter AND to_date_filter
    AND is_sent
    AND is_own_delivery
    )


    , load_dps as (
    SELECT
    platform_order_code
    , entity_id
    , delivery_costs_eur
    , has_subscription
    , exchange_rate
    , has_subscription_discount AS has_subscription_and_discount

    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders`
    WHERE created_date BETWEEN from_date_filter AND to_date_filter
    AND delivery_costs_eur IS NOT NULL
    AND entity_id NOT IN (
    "HS_BH"
    )
    )

    , load_tpro as (
    SELECT
    /*
    TRUE IF user is subscriber and received benefits
    */
    CAST(order_id AS string) as platform_order_code
    , is_tpro_order as tb_has_subscription_and_discount
    , app_version
    , lower(country_Code) as country_code
    FROM `tlb-data-prod.data_platform.fct_order_info`
    WHERE order_date >= date_sub(from_date_filter, interval 2 day)
    AND order_date <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_hs as (
    SELECT
    /*
    is_user_subscribed -> only if user is subscriber
    is_subscribed -> user subscriber + benefits
    */
    CAST(platform_order_code AS STRING) platform_order_code
    , rdf_offer_applied AS is_rdf_order
    , rdf_offer_restaurant_max_charge AS rdf_vendor_contribution
    , OD_delivery_fee - IFNULL(delivery_fee_discount,0) - IFNULL(subscribed_discount_amount,0) as loaded_delivery_fee
    , is_subscribed as hs_has_subscription_and_discount
    , is_user_subscribed as hs_has_subscription
    FROM `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders`
    WHERE operation_day >= date_sub(from_date_filter, interval 2 day)
    AND operation_day <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)


    )

    , load_peya as (
    SELECT
    CAST(order_id AS STRING) as platform_order_code
    , LOWER(country.country_code) AS country_code
    , is_user_plus as peya_has_subscription
    , has_plus_shipping_cost_discount as peya_has_subscription_and_discount
    , shipping_amount as peya_delivery_fee
    FROM `peya-bi-tools-pro.il_core.fact_orders`
    WHERE registered_date >= date_sub(from_date_filter, interval 2 day)
    AND registered_date <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_pd_apac as (
    SELECT
    global_entity_id as entity_id
    , order_code as platform_order_code
    , is_subscriber_order as fp_has_subscription
    , is_subscription_benefit_order as fp_has_subscription_and_benefits
    FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions`
    WHERE created_date_utc >= date_sub(from_date_filter, interval 2 day)
    AND created_date_utc <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_pd_eu as (
    SELECT
    global_entity_id as entity_id
    , order_code as platform_order_code
    , is_subscriber_order as fd_has_subscription
    FROM `fulfillment-dwh-production.pandata_report.regional_eu__pd_orders_agg_sb_subscriptions`
    WHERE created_date_utc >= date_sub(from_date_filter, interval 2 day)
    AND created_date_utc <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )


    , join_tables as (
    SELECT
    c.*
    , cdwh.* EXCEPT(entity_id)
    , dps.delivery_costs_eur

    , CASE
    WHEN hs.is_rdf_order = 1 THEN SAFE_DIVIDE(loaded_delivery_fee,exchange_rate)
    ELSE delivery_fee_eur
    END AS loaded_delivery_fee

    , CASE
    WHEN hs.is_rdf_order = 1 THEN SAFE_DIVIDE(rdf_vendor_contribution,exchange_rate)
    ELSE 0
    END AS vendor_funded_deals_amount

    /*
    THIS FIELD IDENTIFY WETHER THE USER IS A SUBSCRIBER OR NOT!
    As of Q2/2023, for talabat we only know if an order made by susbscription
    users got benefits.
    */
    , CASE
    WHEN has_subscription THEN TRUE
    WHEN hs_has_subscription THEN TRUE
    WHEN peya_has_subscription = 1 THEN TRUE
    WHEN apac.fp_has_subscription THEN TRUE
    WHEN eu.fd_has_subscription THEN TRUE

    WHEN tb_has_subscription_and_discount THEN TRUE
    ELSE has_subscription
    END AS has_subscription
    /*
    Field to use when user is a sub and got benefits
    As of Q2/2023, for Foodora (Europe countries) we only know if an order made by susbscription
    users got benefits.
    */
    , CASE
    WHEN has_subscription_and_discount THEN TRUE
    WHEN hs_has_subscription_and_discount THEN TRUE
    WHEN peya_has_subscription_and_discount = 1 THEN TRUE
    WHEN fp_has_subscription_and_benefits THEN TRUE
    WHEN tb_has_subscription_and_discount = TRUE THEN TRUE
    WHEN fd_has_subscription THEN TRUE
    ELSE has_subscription_and_discount
    END AS has_subscription_and_discount

    FROM load_cdwh cdwh

    INNER JOIN load_dps dps
    ON cdwh.entity_id = dps.entity_id
    AND cdwh.platform_order_code = dps.platform_order_code

    LEFT JOIN countries c
    ON cdwh.entity_id = c.entity_id

    LEFT JOIN load_peya py
    ON c.country_code = py.country_code
    AND cdwh.platform_order_code = py.platform_order_code

    LEFT JOIN load_tpro tb
    ON c.country_code = tb.country_code
    AND cdwh.platform_order_code = tb.platform_order_code

    LEFT JOIN load_hs hs
    ON cdwh.entity_id = "HS_SA"
    AND cdwh.platform_order_code = hs.platform_order_code

    LEFT JOIN load_pd_apac apac
    ON cdwh.entity_id = apac.entity_id
    AND cdwh.platform_order_code = apac.platform_order_code

    LEFT JOIN load_pd_eu eu
    ON cdwh.entity_id = eu.entity_id
    AND cdwh.platform_order_code = eu.platform_order_code
    )

    , add_calculated_fields AS (

      SELECT *

      , service_fee_eur + small_basket_fee_eur as non_df_fees

      --- nominal
      , service_fee_eur + delivery_fee_eur + small_basket_fee_eur as customer_fee_eur
      , service_fee_eur + delivery_fee_eur + small_basket_fee_eur + commission_eur as take_in
      , service_fee_eur + delivery_fee_eur + small_basket_fee_eur + commission_eur - delivery_costs_eur as profit

      --- Loaded
      , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur as loaded_customer_fee_eur
      , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur + vendor_funded_deals_amount + commission_eur as loaded_take_in
      , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur + vendor_funded_deals_amount + commission_eur - delivery_costs_eur as loaded_profit

      FROM join_tables
    )

  ############################################

  ############################################ Aggregate for compliance score purposes

    , aggregate_quarter_performance AS (
      SELECT  
      entity_id
      , management_entity
      /*
      Prev quarter performance is used for the current one
      */
      , DATE_ADD(DATE_TRUNC(created_date_local, QUARTER), INTERVAL 1 QUARTER) AS quarter
      , SAFE_DIVIDE(sum(loaded_customer_fee_eur), SUM(gfv_eur)) as cf_over_afv
      , SAFE_DIVIDE(sum(mov_eur), sum(gfv_eur)) as mov_over_afv
      , SAFE_DIVIDE(sum(commission_eur), sum(gfv_eur)) as comm_rate

      FROM add_calculated_fields
      WHERE TRUE
      AND has_subscription = FALSE
      AND is_qcommerce = FALSE
      GROUP BY 1,2,3
    )

    , get_median as (
      SELECT *
      , PERCENTILE_CONT(mov_over_afv, 0.5) OVER() AS median_mov_over_afv
      , PERCENTILE_CONT(comm_rate, 0.5) OVER() AS median_com_rate
      FROM aggregate_quarter_performance
    )

    , add_market_status as (
      SELECT *
      , IFNULL(cf_over_afv < 0.07, TRUE) AS has_low_cf_over_afv
      , IFNULL(mov_over_afv < median_mov_over_afv,TRUE) as has_low_mov_over_afv
      , comm_rate > median_com_rate AS has_high_comm_rate
      FROM get_median
    )

  ############################################ 

    SELECT *
    FROM add_market_status;

###################################

################################### DDL FOR MANUAL INPUTS

  -- ################################### SUBS MARKTES
  --     CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_subs_markets`
  --         (
  --         quarter DATE,
  --         entity_id STRING,
  --         has_subscription BOOL
  --         )
  --         OPTIONS (
  --         format="GOOGLE_SHEETS",
  --         uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
  --         sheet_range="subs_markets!A:C",
  --         skip_leading_rows=1
  --     );
  -- ###################################


  -- ################################### COMPETITIVE INPUTS
  --     CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_competitive_inputs`
  --         (
  --         quarter DATE,
  --         entity_id STRING,
  --         competitor_has_sf BOOL
  --         )
  --         OPTIONS (
  --         format="GOOGLE_SHEETS",
  --         uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
  --         sheet_range="comp_inputs!A:C",
  --         skip_leading_rows=1
  --     );
  -- ###################################

  -- ################################### FLEET CONSTRAINTS
  --     CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_fleet_constraints_inputs`
  --         (
  --         quarter DATE,
  --         entity_id STRING,
  --         has_fleet_constraints BOOL,
  --         has_extreme_fleet_constraints BOOL
  --         )
  --         OPTIONS (
  --         format="GOOGLE_SHEETS",
  --         uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
  --         sheet_range="fleet_constraints!A:D",
  --         skip_leading_rows=1
  --     );
  -- ###################################

  -- ################################### MARKET ARCHETYPE STATUS
  --     CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_market_archetype`
  --         (
  --         quarter DATE,
  --         entity_id STRING,
  --         market_archetype STRING,
  --         )
  --         OPTIONS (
  --         format="GOOGLE_SHEETS",
  --         uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
  --         sheet_range="market_leadership!A:C",
  --         skip_leading_rows=1
  --     );
  -- ###################################

  -- ################################### LEGAL STATUS
  --     CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_legal_status`
  --         (
  --         quarter DATE,
  --         entity_id STRING,
  --         mechanism_code STRING,
  --         is_legal BOOL
  --         )
  --         OPTIONS (
  --         format="GOOGLE_SHEETS",
  --         uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
  --         sheet_range="pm_legality!A:D",
  --         skip_leading_rows=1
  --     );
  -- ###################################



###################################

################################### EVALUATION PER COUNTRY
  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compscore_evaluation_per_country` AS

  ################################# LOAD SOURCES

        with markets AS (
              SELECT
              entity_id
              FROM UNNEST([
                    "DJ_CZ"
                    ,"FO_NO"
                    ,"FP_SK"
                    ,"HN_DK"
                    ,"MJM_AT"
                    ,"NP_HU"
                    ,"OP_SE"
                    ,"PO_FI"
                    ,"YS_TR"
                    ,"EF_GR"
                    ,"FY_CY"
                    ,"FP_BD"
                    ,"FP_HK"
                    ,"FP_KH"
                    ,"FP_LA"
                    ,"FP_MM"
                    ,"FP_MY"
                    ,"FP_PH"
                    ,"FP_PK"
                    ,"FP_SG"
                    ,"FP_TH"
                    ,"FP_TW"
                    ,"HS_SA"
                    ,"AP_PA"
                    ,"PY_AR"
                    ,"PY_BO"
                    ,"PY_CL"
                    ,"PY_CR"
                    ,"PY_DO"
                    ,"PY_EC"
                    ,"PY_GT"
                    ,"PY_HN"
                    ,"PY_NI"
                    ,"PY_PE"
                    ,"PY_PY"
                    ,"PY_SV"
                    ,"PY_UY"
                    ,"PY_VE"
                    ,"HF_EG"
                    ,"TB_AE"
                    ,"TB_BH"
                    ,"TB_IQ"
                    ,"TB_JO"
                    ,"TB_KW"
                    ,"TB_OM"
                    ,"TB_QA"
              ]) as entity_id
        )
        
        , quarters AS (
              SELECT
              quarter
              FROM UNNEST(
              GENERATE_DATE_ARRAY("2023-07-01", "2025-01-01",INTERVAL 1 QUARTER)
              ) as quarter
              WHERE TRUE
              AND quarter <= CURRENT_DATE()
        )

        , cross_join_markets_quarters AS (
              SELECT quarter
              , entity_id
              FROM markets 
              CROSS JOIN quarters
        )

        , subs_markets AS (
              SELECT *
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_subs_markets`
        )

        , comp_inputs AS (
              SELECT *
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_competitive_inputs`
        )

        , fleet_constraints AS (
              SELECT *
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_fleet_constraints_inputs`
        )

        , market_leadership AS (
              SELECT *
              , LOWER(market_archetype) LIKE "%leadership%"  as has_leadership
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_market_archetype`
        )

        , legality AS (
              SELECT *
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_legal_status`
        )

        , performance_inputs AS (
              SELECT *
              FROM `logistics-data-storage-staging.long_term_pricing.compscore_performance_inputs`
        )


  #################################

  ################################# DECISION TREES

        , dbdf_tree AS (
              SELECT 
              market.*
              , "dbdf" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market
              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter  
                    AND legality.mechanism_code = "dbdf"   
        )

        , soft_mov_tree AS (
              SELECT 
              market.*
              , "soft_mov" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market
              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "soft_mov"   
        
        )

        , fdnc_tree AS (
              SELECT 
              market.*
              , "fdnc" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market
              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "fdnc"   
        
        )

        , time_conditions_tree AS (
              SELECT 
              market.*
              , "tod_dow" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market
              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "tod_dow"   
        )

        , sbf_tree AS (
              SELECT 
              market.*
              , "sbf" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market
              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "sbf"   
        )

        , service_fee_tree AS (
              SELECT 
              market.*
              , "service_fee" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    AND (
                          IFNULL(sm.has_subscription,FALSE)
                          OR  
                          IFNULL(ml.has_leadership,FALSE)
                          OR 
                          IFNULL(cp.competitor_has_sf,FALSE)
                    )
                    THEN TRUE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "service_fee"  

              LEFT JOIN subs_markets sm
                    ON market.entity_id = sm.entity_id
                    AND market.quarter = sm.quarter

              LEFT JOIN market_leadership ml
                    ON market.entity_id = ml.entity_id
                    AND market.quarter = ml.quarter
              
              LEFT JOIN comp_inputs cp
                    ON market.entity_id = cp.entity_id
                    AND market.quarter = cp.quarter
        )

        , bvdf_tree AS (
              SELECT 
              market.*
              , "bvdf" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    THEN 
                          CASE 
                                WHEN IFNULL(ml.has_leadership, FALSE) THEN FALSE
                                WHEN pi.has_low_cf_over_afv AND pi.has_high_comm_rate THEN TRUE
                                ELSE FALSE
                          END
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "bvdf"  

              LEFT JOIN market_leadership ml
                    ON market.entity_id = ml.entity_id
                    AND market.quarter = ml.quarter

              LEFT JOIN performance_inputs pi
                    ON market.entity_id = pi.entity_id
                    AND market.quarter = pi.quarter
        )

        , dbmov_tree AS (
              SELECT 
              market.*
              , "dbmov" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    THEN 
                          CASE 
                                WHEN IFNULL(ml.has_leadership, FALSE) THEN TRUE
                                ELSE FALSE
                          END
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "dbmov"  

              LEFT JOIN market_leadership ml
                    ON market.entity_id = ml.entity_id
                    AND market.quarter = ml.quarter
        )

        , surge_tree AS (
              SELECT 
              market.*
              , "surge" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    THEN 
                          CASE 
                                WHEN IFNULL(fc.has_fleet_constraints, FALSE) THEN TRUE
                                ELSE FALSE
                          END
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "surge"  

              LEFT JOIN fleet_constraints fc
                    ON market.entity_id = fc.entity_id
                    AND market.quarter = fc.quarter
        )

        , customer_location_tree AS (
              SELECT 
              market.*
              , "customer_location" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    THEN FALSE
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "customer_location"  
              

        )

        , priority_tree AS (
              SELECT 
              market.*
              , "priority_fee" as mechanism_code
              , CASE 
                    WHEN IFNULL(is_legal, TRUE) 
                    THEN CASE
                          WHEN IFNULL(ml.has_leadership, FALSE) 
                                AND IFNULL(fc.has_extreme_fleet_constraints, FALSE) = FALSE
                                THEN TRUE
                          ELSE FALSE
                          END
                    ELSE FALSE
              END AS should_have
              FROM cross_join_markets_quarters market

              LEFT JOIN legality
                    ON market.entity_id = legality.entity_id
                    AND market.quarter = legality.quarter    
                    AND legality.mechanism_code = "priority_fee" 

              LEFT JOIN market_leadership ml
                    ON market.entity_id = ml.entity_id
                    AND market.quarter = ml.quarter

              LEFT JOIN fleet_constraints fc
                    ON market.entity_id = fc.entity_id
                    AND market.quarter = fc.quarter
        )

  #################################

  ################################# UNION RESULTS
        , combine_trees AS (
              SELECT *
              FROM dbdf_tree

              UNION ALL

              SELECT *
              FROM soft_mov_tree

              UNION ALL 

              SELECT *
              FROM fdnc_tree

              UNION ALL

              SELECT *
              FROM time_conditions_tree

              UNION ALL

              SELECT *
              FROM sbf_tree

              UNION ALL

              SELECT *
              FROM service_fee_tree

              UNION ALL

              SELECT *
              FROM bvdf_tree

              UNION ALL

              SELECT *
              FROM dbmov_tree

              UNION ALL

              SELECT *
              FROM surge_tree

              UNION ALL

              SELECT *
              FROM customer_location_tree

              UNION ALL

              SELECT *
              FROM priority_tree
        )


  #################################
        
  SELECT *
  FROM combine_trees;
###################################

################################### SCORE DETAILED PER COUNTRY

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compliance_score_tracking` AS

  with load_mechanism_to_comply AS (
    SELECT 
    quarter
  , entity_id
  , should_have
  -- , CAST(should_have as BOOL) as should_have
  , mechanism_code as mechanism
  FROM `logistics-data-storage-staging.long_term_pricing.compscore_evaluation_per_country`
  -- WHERE should_have IN ("TRUE", "FALSE")
  )

  , entities_dim as (
    SELECT
    global_entity_id as entity_id
    , management_entity
    FROM `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
  )

  , mechanism_in_use as (
        select
        DATE_TRUNC(init_week, QUARTER) as quarter,
        p.init_week,
        p.entity_id,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_dbdf then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as dbdf,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_fleet_delay OR order_price_mechanisms.is_surge_mov then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as surge,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_basket_value_deal then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as bvdf,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_service_fee then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as service_fee,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_small_order_fee then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as sbf,
        SAFE_DIVIDE(count(case when order_price_mechanisms.is_dbmov then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as dbmov,
        SAFE_DIVIDE(count(case when vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as priority_fee,
        SAFE_DIVIDE(count(case when vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as tod_dow,
        SAFE_DIVIDE(count(case when vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as fdnc,
        SAFE_DIVIDE(count(case when vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end), COUNT(platform_order_code)) >= 0.5 as customer_location,
        SAFE_DIVIDE(count(case when only_dps_scheme_price_mechanisms.mov_type = "Flat_zero" then platform_order_code end), COUNT(platform_order_code)) < 0.5 as soft_mov
      from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
      where vendor_price_scheme_type <> "Campaign"
      AND init_week >= filter_period
      AND vertical_parent = "Food"
      AND is_own_delivery
      AND entity_id NOT IN ("FP_DE")
      GROUP BY 1,2,3
  )


  , unpivot_mechanism_in_use AS (
    SELECT *
    FROM mechanism_in_use
    UNPIVOT(in_use FOR mechanism in (
      dbdf
      , surge
      , bvdf
      , service_fee
      , sbf
      , dbmov
      , priority_fee
      , tod_dow
      , fdnc
      , customer_location
      , soft_mov
      )
    )
  )

  , join_values AS (
    SELECT a.*
    , ed.management_entity
    -- , opm.n_orders
    , CASE 
        WHEN a.in_use = TRUE THEN 1 
        WHEN b.should_have = TRUE THEN 0
        ELSE NULL 
    END  as is_compliant
    , CASE 
        WHEN a.in_use = TRUE THEN 1 
        WHEN b.should_have = TRUE THEN 1
        ELSE 0
    END AS should_have

    FROM unpivot_mechanism_in_use a

    LEFT JOIN load_mechanism_to_comply b
      ON a.entity_id = b.entity_id
      AND a.mechanism = b.mechanism
      AND a.quarter = b.quarter

    LEFT JOIN entities_dim ed
      ON a.entity_id = ed.entity_id
      
  )


  SELECT *
  FROM join_values;
###################################

################################### SCORE PER PLATFORM / GLOBAL

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compliance_score_tracking_aggregated` AS
  with load_comp_score AS (
    SELECT 
    management_entity
    , init_week
    , entity_id
    , SUM(is_compliant) as n_in_use
    , SUM(should_have) as n_mechanism
    , SAFE_DIVIDE(SUM(is_compliant), SUM(should_have)) as entity_comp_score
    FROM `logistics-data-storage-staging.long_term_pricing.compliance_score_tracking`
    GROUP BY 1,2,3
  )

  , orders_per_market AS (
        select
        p.init_week,
        p.entity_id,
        COUNT(platform_order_code) as entity_orders
      from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
      where vendor_price_scheme_type <> "Campaign"
      AND init_week >= DATE_SUB(filter_period, INTERVAL 12 WEEK)
      AND vertical_parent = "Food"
      AND is_own_delivery
      GROUP BY 1,2
  )

  , get_last_x_weeks_orders AS (
    SELECT *
      , SUM(entity_orders) OVER(
      PARTITION BY entity_id 
      ORDER BY UNIX_DATE(init_week) 
      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) as entity_orders_2_week
    FROM orders_per_market
  )

  SELECT lcs.*
  , entity_orders
  , entity_orders_2_week
  from load_comp_score lcs
  LEFT JOIN  get_last_x_weeks_orders opm
    ON lcs.entity_id = opm.entity_id
    AND lcs.init_week = opm.init_week


###################################
