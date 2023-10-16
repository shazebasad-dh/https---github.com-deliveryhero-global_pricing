

/*
file with dimension https://docs.google.com/spreadsheets/d/1Osy618G58aUDFXwa4VWUUBHymieInOwXTZ-hSuMUWGk/edit#gid=0
*/

-- DECLARE filter_period DATE DEFAULT "2023-03-06";
DECLARE filter_period DATE DEFAULT "2023-09-11";



###################################
    -- CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_evaluation_per_country`
    --     (
    --     entity_id STRING,
    --     should_have STRING,
    --     mechanism STRING
    --     )
    --     OPTIONS (
    --     format="GOOGLE_SHEETS",
    --     uris=["https://docs.google.com/spreadsheets/d/1Osy618G58aUDFXwa4VWUUBHymieInOwXTZ-hSuMUWGk/edit#gid=876550049"],
    --     sheet_range="import_to_bq!A1:C1000",
    --     skip_leading_rows=1
    -- );
###################################


###################################

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compliance_score_tracking` AS

  with load_mechanism_to_comply AS (
    SELECT 
  entity_id
  , CAST(should_have as BOOL) as should_have
  , mechanism
  FROM `logistics-data-storage-staging.long_term_pricing.compscore_evaluation_per_country`
  WHERE should_have IN ("TRUE", "FALSE")
  )

  , entities_dim as (
    SELECT
    global_entity_id as entity_id
    , management_entity
    FROM `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
  )

  , mechanism_in_use as (
        select
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
      GROUP BY 1,2
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

    -- LEFT JOIN orders_per_market opm
    --   ON a.entity_id = opm.entity_id
    --   AND a.init_week = opm.init_week
    
    LEFT JOIN entities_dim ed
      ON a.entity_id = ed.entity_id
      
  )


  SELECT *
  FROM join_values;
###################################

###################################

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
      AND init_week >= DATE_SUB(filter_period, INTERVAL 4 WEEK)
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
