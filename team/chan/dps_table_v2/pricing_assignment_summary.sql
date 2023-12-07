  -----------------------------------------------------------------------------------------------------------------------
-- Name            : pricing_mech_id.sql
-- Initial Author  : Sebastian Lafaurie
-- Owner           : Global Pricing Team
-- Initial Create  : -
-- Additional Notes: -
-- UPDATED         : 2022-04-25 | Sebastian Lafaurie | First commit
--                   2022-04-27 | Sebastian Lafaurie | Add manual fix on HS_SA FDNC.
--                   2022-05-04 | Sebastian Lafaurie | Split Flat and Var MOV logic.
--                   2022-05-11 | Sebastian Lafaurie | Add FDNC new logic
--                   2022-06-30 | Sebastian Lafaurie | Fix TOD Logic for non-variant metric. Improve Fleet Delay count
--                   2022-09-07 | Sebastian Lafaurie | Improve DBDF, MOV and Fleet Delay by using component history
--                   2022-09-13 | Sebastian Lafaurie | Add Surge MOV, Small Basket Fee and Service Fee Europe (hard-coded)
--                   2022-09-20 | Sebastian Lafaurie | Add SBF APAC, SF AT (hard-coded)
--                   2022-09-28 | Sebastian Lafaurie | Improve TOD using with_time_condition.
--                   2022-09-28 | Sebastian Lafaurie | Refactor into incremental table.
--                   2022-11-20 | Sebastian Lafaurie | Add vendor level metrics, backfill logic, use vendor_price_variant instead.
--                   2023-02-15 | Sebastian Lafaurie | Include DPS ASA tables into the calculation logic.
--                   2023-04-24 | Sebastian Lafaurie | Include Service Fee Overrides, HS RDF and discern into DPS + Local Mechanisms
--                   2023-05-10 | Sebastian Lafaurie | Add Priority Delivery tag for PeYa
--                   2023-07-20 | Sebastian Lafaurie | Change FDNC campaign measurement to configuration based
--                   2023-09-20 | Sebastian Lafaurie | Add Service Fee Overrides
--                   2023-11-01 | Sebastian Lafaurie | Add Priority Delivery Tags for Pandora
--                   2023-11-27 | Sebastian Lafaurie | Set RDF in HS as fixed value. Remove dependency on non-prd sources
-----------------------------------------------------------------------------------------------------------------------
################################################## DECLARE VARIABLES
DECLARE date_partition DATE;
DECLARE current_week DATE;
DECLARE week_interval INT64;
DECLARE backfill BOOL;

# SET RUN MODE
SET backfill = FALSE;

# SET END DATE
SET current_week = DATE_TRUNC(CURRENT_DATE(), ISOWEEK);

# SET PARTITION DATE
IF backfill THEN
    SET date_partition = DATE_SUB("2022-01-01", INTERVAL 1 WEEK);
      -- SET date_partition = DATE_SUB(current_week, interval 4 WEEK);
  ELSE
    SET date_partition = DATE_SUB(current_week, INTERVAL 5 WEEK);
END IF
;

# PRINT INPUTS
SELECT
  backfill AS backfill,
  date_partition AS from_date,
  current_week AS to_date 
;

##################################################
  ################################################## CREATE STAGING TABLE
CREATE OR REPLACE TEMP TABLE staging_data AS

WITH 

  countries AS (
    SELECT DISTINCT 
      segment AS region,
      management_entity,
      country_name,
      global_entity_id AS entity_id
    FROM 
      `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` 
  )
  
  , load_vendors_parent_vertical AS (
    SELECT
      vertical_parent,
      vendor_id,
      global_entity_id AS entity_id
    FROM
      `fulfillment-dwh-production.curated_data_shared_coredata_business.vendors` 
  )

  , load_vendor_price_data AS (
    SELECT
      entity_id /*
        Includes service fee overrides
        */,
      vendor_code AS vendor_id,
      active_from,
      IFNULL(active_to, CURRENT_TIMESTAMP) AS active_to,
      vendor_price_mechanisms.vendor_has_dbdf,
      vendor_price_mechanisms.vendor_has_dbmov,
      vendor_price_mechanisms.vendor_has_fleet_delay,
      vendor_price_mechanisms.vendor_has_small_order_fee,
      vendor_price_mechanisms.vendor_has_surge_mov,
      vendor_price_mechanisms.vendor_has_basket_value_deal,
      vendor_price_mechanisms.vendor_has_service_fee,
      vendor_price_mechanisms.vendor_has_time_condition,
      vendor_price_mechanisms.vendor_has_customer_condition,
      vendor_price_mechanisms.vendor_has_customer_area
    FROM
      `logistics-data-storage-staging.long_term_pricing.vendor_full_pricing_configuration_versions` 
    
  )

  , load_fdnc_vendors AS (
      SELECT 
        entity_id
        , vendor_code
        , campaign_active_from
        , campaign_active_to
        , ARRAY_AGG(
          STRUCT(
            campaign_id
            , campaign_name
            , customer_condition_config.counting_method
            , CASE 
                  WHEN customer_condition_config.counting_method IS NULL THEN TRUE
                  WHEN customer_condition_config.counting_method = "TOTAL" THEN TRUE
                  ELSE FALSE
              END AS is_total_customer_condition
            , schedule_config.start_at
            , schedule_config.timezone
            , IFNULL(schedule_config.recurrence_end_at, schedule_config.end_at) as schedule_config_end
          )
        ) as fdnc_campaigns
      FROM 
        `fulfillment-dwh-production.cl.vendor_campaign_configuration_versions`
      LEFT JOIN 
        UNNEST(dps_configuration_history) config
      WHERE TRUE
        AND vendor_code IS NOT NULL
        AND vendor_code <> ""
        /*
        We only want to consider campaigns with a customer condition.
        */
        AND customer_condition_id IS NOT NULL
        AND customer_condition_config.description IS NOT NULL 
        GROUP BY 
          1,2,3,4
  )

  , load_asa_condition_data AS (
    SELECT
      entity_id,
      asa_id,
      active_from,
      IFNULL(active_to,CURRENT_TIMESTAMP) active_to,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(asa_price_config) apc WHERE apc.schedule_id IS NOT NULL) AS asa_time_condition_schemes,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(asa_price_config) apc WHERE apc.customer_condition_id IS NOT NULL) AS asa_customer_condition_schemes,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(asa_price_config) apc WHERE apc.n_areas > 0 ) AS asa_customer_location_schemes
    FROM
      `fulfillment-dwh-production.cl._pricing_asa_configuration_versions` 
  )

  , load_exp_condition_data AS (
    SELECT
      entity_id,
      test_id,
      variation_group,
      ARRAY_AGG( STRUCT( price_scheme_id AS scheme_id,
          customer_condition.id AS customer_condition_id,
          schedule.id AS schedule_id,
          ARRAY_LENGTH(customer_areas) AS n_areas ) ) AS test_price_config
    FROM
      `fulfillment-dwh-production.cl.dps_experiment_setups`
    GROUP BY 1, 2, 3 
  )

  , experiment_condition_clean AS (
    SELECT
      entity_id,
      test_id,
      variation_group AS vendor_price_variant,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(test_price_config) apc WHERE apc.schedule_id IS NOT NULL) AS test_time_condition_schemes,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(test_price_config) apc WHERE apc.customer_condition_id IS NOT NULL) AS test_customer_condition_schemes,
      ARRAY(SELECT DISTINCT scheme_id FROM UNNEST(test_price_config) apc WHERE apc.n_areas > 0) AS test_customer_location_schemes
    FROM
      load_exp_condition_data 
  )

  , add_test_has_condition_mechanisms AS (
    SELECT
      *,
      ARRAY_LENGTH(test_time_condition_schemes) > 0 AS test_has_time_condition,
      ARRAY_LENGTH(test_customer_condition_schemes) > 0 AS test_has_customer_condition,
      ARRAY_LENGTH(test_customer_location_schemes) > 0 AS test_has_customer_location
    FROM
      experiment_condition_clean 
  )

  , load_scheme_data AS (
    SELECT
      entity_id,
      scheme_id,
      scheme_active_from,
      IFNULL(scheme_active_to, CURRENT_TIMESTAMP()) scheme_active_to,
      scheme_price_mechanisms.is_dbdf,
      scheme_price_mechanisms.is_dbmov,
      scheme_price_mechanisms.is_surge_mov,
      scheme_price_mechanisms.is_small_order_fee,
      scheme_price_mechanisms.is_fleet_delay,
      scheme_price_mechanisms.is_basket_value_deal,
      scheme_price_mechanisms.is_service_fee,
      scheme_price_mechanisms.mov_type,
      scheme_price_mechanisms.service_fee_type
    FROM
      `fulfillment-dwh-production.cl.pricing_configuration_versions` 
  )

  , load_prio_delivery AS (
    SELECT DISTINCT 
      global_order_id AS platform_order_code,
      entity_id,
      TRUE AS has_priority_delivery
    FROM
      `fulfillment-dwh-production.cl.orders_v2`
    LEFT JOIN
      UNNEST(rider.tags) tags
    WHERE
      created_date >= date_partition - INTERVAL 2 WEEK
      AND tags IN ('PRIORITIZE_DELIVERY' --peya
                    , 'priority_delivery' --pandora
                  ) 
  )


  #####
  ##### LOAD ORDER DATA AND JOIN PM DATA
  , dps_order_raw AS (
    SELECT
      entity_id,
      DATE_TRUNC(created_date_local, ISOWEEK) AS init_week,
      -- Week start monday
      vertical_type,
      platform_order_code,
      created_date_local,
      order_placed_at,
      CAST(scheme_id AS INT64) AS scheme_id,
      vendor_price_variant,
      vendor_id,
      is_own_delivery,
      zone_id,
      assignment_id,
      city_name,
      delivery_fee_local,
      delivery_fee_eur,
      service_fee_local,
      service_fee_eur,
      mov_customer_fee_local,
      has_time_condition AS with_time_condition,
      has_new_customer_condition AS with_customer_condition,
      has_customer_area_condition AS with_customer_area_condition,
      vendor_price_scheme_type,
      components
    FROM
      `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders`
    WHERE
      created_date >= date_partition - INTERVAL 1 WEEK
      AND created_date_local >= date_partition
      AND created_date_local <= current_week
      AND is_sent
      AND scheme_id IS NOT NULL 
  )
  
  , dps_order AS (
    SELECT 
      *
    FROM
      dps_order_raw
    LEFT JOIN
      load_vendors_parent_vertical
    USING
      (vendor_id, entity_id) 
  )

  , dps_order_with_pm AS (
    SELECT
      c.* EXCEPT(entity_id),
      dps.*,
      s.* EXCEPT(entity_id, vendor_id, active_from, active_to),
      sch.* EXCEPT(entity_id, scheme_id, scheme_active_from, scheme_active_to),
      asa.* EXCEPT(entity_id, asa_id, active_from, active_to),
      exper.* EXCEPT(entity_id, test_id, vendor_price_variant),  
      IFNULL(prio.has_priority_delivery, FALSE) AS has_priority_delivery,
      IFNULL(fdnc.vendor_has_customer_condition_in_campaign,FALSE) AS vendor_has_customer_condition_in_campaign
    FROM dps_order dps
    
    LEFT JOIN countries c
      USING (entity_id)

    LEFT JOIN load_vendor_price_data s
      ON dps.entity_id = s.entity_id
      AND dps.vendor_id = s.vendor_id
      AND order_placed_at >= s.active_from
      AND order_placed_at < s.active_to
    
    LEFT JOIN load_scheme_data sch
      ON dps.entity_id = sch.entity_id
      AND dps.scheme_id = sch.scheme_id
      AND order_placed_at >= scheme_active_from
      AND order_placed_at < scheme_active_to
    
    LEFT JOIN load_asa_condition_data asa
      ON dps.entity_id = asa.entity_id
      AND dps.assignment_id = asa.asa_id
      AND dps.vendor_price_scheme_type = "Automatic scheme"
      AND order_placed_at >= asa.active_from
      AND order_placed_at < asa.active_to
    
    LEFT JOIN add_test_has_condition_mechanisms exper
      ON dps.entity_id = exper.entity_id
      AND dps.assignment_id = exper.test_id
      AND dps.vendor_price_variant = exper.vendor_price_variant
      AND dps.vendor_price_scheme_type = "Experiment"

    LEFT JOIN load_prio_delivery prio
      ON dps.entity_id = prio.entity_id
      AND dps.platform_order_code = prio.platform_order_code
    
    LEFT JOIN load_fdnc_vendors fdnc
      ON dps.entity_id = fdnc.entity_id
      AND dps.vendor_id = fdnc.vendor_code
      AND order_placed_at >= fdnc.fdnc_valid_from
      AND order_placed_at < fdnc.fdnc_valid_to 
  )
  #####
  ##### APPLY PM LOGIC
  /*
      The two CTE makes more robust the count of conditions mechanisms
      Customer are usually applied through Campaigns. 
      For all conditions, the with_X column are only available since Sep22
      */
  -- , count_weekly_fdnc as (
  --     --- fix to account for FDNC that's applied from campaign
  --     select
  --         init_week,
  --         region,
  --         entity_id,
  --         vendor_id,
  --         COUNT(CASE when with_customer_condition then platform_order_code end) as fdnc_order_qty,
  --         -- count(case when service_fee_local > 0 then platform_order_code end) as service_fee_order_qty,
  --         -- count(case when mov_customer_fee_local > 0 then platform_order_code end) as sof_order_qty
  --     from dps_order_with_pm
  --     where vendor_price_scheme_type = "Campaign" -- only useful use for campaigns
  --     group by 1,2,3,4
  -- )
  ,
  weekly_prio_order AS ( /*
        Priority is enabled at the country level, so if a country has just one prio order 
        all orders must have the ability to be prio
        */
  SELECT
    DISTINCT init_week,
    entity_id,
    zone_id,
    is_own_delivery,
    has_priority_delivery AS vendor_has_priority_delivery
  FROM
    dps_order_with_pm
  WHERE
    has_priority_delivery = TRUE
    AND is_own_delivery ),
  make_condition_mech_robust AS (
  SELECT
    * EXCEPT( vendor_has_customer_condition,
      vendor_has_time_condition,
      vendor_has_customer_area,
      is_service_fee ),
    CASE
      WHEN vendor_has_customer_condition_in_campaign THEN TRUE
      WHEN vendor_price_scheme_type = "Experiment" THEN test_has_customer_condition
    ELSE
    vendor_has_customer_condition
  END
    AS vendor_has_customer_condition,
    CASE
      WHEN vendor_price_scheme_type = "Experiment" THEN test_has_time_condition
    ELSE
    vendor_has_time_condition
  END
    AS vendor_has_time_condition,
    CASE
      WHEN vendor_price_scheme_type = "Experiment" THEN test_has_customer_location
    ELSE
    vendor_has_customer_area
  END
    AS vendor_has_customer_area,
    CASE
      WHEN with_customer_condition THEN TRUE
    --- for before Sep22, when with_customer_condition wasn't available.
    /*
            IF the applied scheme is tied to an condition, then the condition
            must have been TRUE at that moment. 
            */
      WHEN scheme_id IN UNNEST(asa_customer_condition_schemes) THEN TRUE
      WHEN scheme_id IN UNNEST(test_customer_condition_schemes) THEN TRUE
    ELSE
    FALSE
  END
    AS is_fdnc,
    CASE
      WHEN with_time_condition THEN TRUE
    --- for before Sep22, when with_time_condition wasn't available
      WHEN scheme_id IN UNNEST(asa_time_condition_schemes) THEN TRUE
      WHEN scheme_id IN UNNEST(test_time_condition_schemes) THEN TRUE
    ELSE
    FALSE
  END
    AS is_tod,
    CASE
      WHEN with_customer_area_condition THEN TRUE
      WHEN scheme_id IN UNNEST(asa_customer_location_schemes) THEN TRUE
      WHEN scheme_id IN UNNEST(test_customer_location_schemes) THEN TRUE
    ELSE
    FALSE
  END
    AS is_customer_location,
    CASE
      WHEN components.service_fee_id IS NOT NULL THEN TRUE
    ELSE
    is_service_fee
  END
    AS is_service_fee
  FROM
    dps_order_with_pm
    -- LEFT JOIN count_weekly_fdnc
    -- USING(init_week, region, entity_id, vendor_id)
  LEFT JOIN
    weekly_prio_order
  USING
    (init_week,
      entity_id,
      zone_id,
      is_own_delivery) ) /*
      The Following CTEs adjust PM to take into account
      local implementations:
      - Pandora's Top Up is considered as SBF
      - HungerStation's RDF is a Basket Value
      - Some countries in Europe implements Service Fee outside DPS
      */
  -- , weekly_rdf_vendors as (
  --     SELECT DISTINCT
  --     init_week
  --     , entity_id
  --     , vendor_id
  --     , TRUE as is_rdf_vendor
  --     FROM dps_order_with_pm
  --     WHERE is_rdf_order = 1
  --     GROUP BY 1,2,3
  -- )
  ,
  add_manual_fixes AS (
  SELECT
    *
    ------ VENDOR CONFIGURATION
    ----- FDNC
    ,
    CASE
      WHEN entity_id = "HS_SA" THEN TRUE
    ELSE
    vendor_has_customer_condition
  END
    AS vendor_has_customer_condition_adjusted
    ----- Service Fee
    ,
    CASE
      WHEN country_name IN ("Sweden", "Norway", "Finland", "Czech Republic") THEN TRUE
      WHEN country_name = "Austria"
    AND order_placed_at >= "2022-09-22" THEN TRUE
      WHEN entity_id IN ("FP_SK", "NP_HU") AND order_placed_at>= "2023-01-01" THEN TRUE
    ELSE
    vendor_has_service_fee
  END
    AS vendor_has_service_fee_adjusted
    ----- SOF
    ,
    CASE
      WHEN region = "Europe" AND entity_id NOT IN ('NP_HU', "FY_CY", "EF_GR") THEN TRUE
      WHEN region = "Asia" THEN TRUE
    ELSE
    vendor_has_small_order_fee
  END
    AS vendor_has_small_order_fee_adjusted
    ----- BASKET VALUE DEALS
    /*
          RDF offers in HS are considered BVD
          */,
    CASE
      WHEN entity_id = "HS_SA" THEN TRUE
    ELSE
    vendor_has_basket_value_deal
  END
    AS vendor_has_basket_value_deal_adjusted
    ------ APPLIED MECHANISM
    ,
    CASE
      WHEN entity_id = "HS_SA" THEN TRUE
    ELSE
    is_fdnc
  END
    AS is_fdnc_adjusted
    ----- SERVICE FEE
    ,
    CASE
      WHEN country_name IN ("Sweden", "Norway", "Finland", "Czech Republic") THEN TRUE
      WHEN country_name = "Austria"
    AND order_placed_at >= "2022-09-22" THEN TRUE
      WHEN entity_id IN ("FP_SK", "NP_HU") AND order_placed_at>= "2023-01-01" THEN TRUE
      WHEN entity_id IN ("FY_CY")
    AND order_placed_at>="2023-08-01" THEN TRUE
    ELSE
    is_service_fee
  END
    AS is_service_fee_adjusted
    ----- SOF
    ,
    CASE
      WHEN region = "Europe" AND entity_id NOT IN ('NP_HU', "FY_CY", "EF_GR") THEN TRUE
      WHEN region = "Asia" THEN TRUE
    ELSE
    is_small_order_fee
  END
    AS is_small_order_fee_adjusted
    ----- BASKET VALUE DEALS
    /*
          RDF offers in HS are considered BVD
          */,
    CASE
      WHEN entity_id = "HS_SA" THEN TRUE
    ELSE
    is_basket_value_deal
  END
    AS is_basket_value_deal_adjusted
    ------ APPLIED MECHANISM
  FROM
    make_condition_mech_robust
    -- LEFT JOIN weekly_rdf_vendors
    --   USING(init_week, entity_id, vendor_id)
    ),
  add_number_of_mechanisms AS (
  SELECT
    *
    ---- ONLY DPS MECHANISMS FIELDS
    ---- CONFIGURED
    ,
  IF
    (vendor_has_dbdf,1,0) +
  IF
    (vendor_has_time_condition,1,0) +
  IF
    (vendor_has_fleet_delay,1,0) +
  IF
    (vendor_has_basket_value_deal, 1,0) +
  IF
    (vendor_has_service_fee,1,0) +
  IF
    (vendor_has_customer_condition,1,0) +
  IF
    (vendor_has_small_order_fee,1,0) +
  IF
    (vendor_has_dbmov, 1,0) +
  IF
    (vendor_has_surge_mov, 1,0) +
  IF
    (vendor_has_customer_area,1,0) AS vendor_asa_price_mechanism_count_only_dps
    ---- EXPOSED
    ,
  IF
    (is_dbdf,1,0) +
  IF
    (is_fleet_delay,1,0) +
  IF
    (is_basket_value_deal, 1,0) +
  IF
    (is_service_fee,1,0) +
  IF
    (is_small_order_fee,1,0) +
  IF
    (is_dbmov, 1,0) +
  IF
    (is_surge_mov, 1,0) +
  IF
    (vendor_has_customer_area,1,0) +
  IF
    (vendor_has_time_condition,1,0) +
  IF
    (vendor_has_customer_condition,1,0) AS exposed_price_mechanism_count_only_dps
    ---- APPLIED
    ,
  IF
    (is_dbdf, 1,0) +
  IF
    (is_dbmov, 1,0) +
  IF
    (is_surge_mov, 1,0) +
  IF
    (is_small_order_fee, 1,0) +
  IF
    (is_fleet_delay, 1,0) +
  IF
    (is_basket_value_deal, 1,0) +
  IF
    (is_service_fee, 1,0) +
  IF
    (is_tod,1,0) +
  IF
    (is_fdnc,1,0) +
  IF
    (is_customer_location, 1,0) AS order_price_mechanism_count_only_dps
    ----
    ---- ADJUSTED MECHANISMS
    ---- CONFIGURED
    ,
  IF
    (vendor_has_dbdf,1,0) +
  IF
    (vendor_has_time_condition,1,0) +
  IF
    (vendor_has_fleet_delay,1,0) +
  IF
    (vendor_has_basket_value_deal_adjusted, 1,0) +
  IF
    (vendor_has_service_fee_adjusted,1,0) +
  IF
    (vendor_has_customer_condition_adjusted,1,0) +
  IF
    (vendor_has_small_order_fee_adjusted,1,0) +
  IF
    (vendor_has_dbmov, 1,0) +
  IF
    (vendor_has_surge_mov, 1,0) +
  IF
    (vendor_has_customer_area,1,0) +
  IF
    (vendor_has_priority_delivery, 1,0) AS vendor_asa_price_mechanism_count
    ---- EXPOSED
    ,
  IF
    (is_dbdf,1,0) +
  IF
    (is_fleet_delay,1,0) +
  IF
    (is_basket_value_deal_adjusted, 1,0) +
  IF
    (is_service_fee_adjusted,1,0) +
  IF
    (is_small_order_fee_adjusted,1,0) +
  IF
    (is_dbmov, 1,0) +
  IF
    (is_surge_mov, 1,0) +
  IF
    (vendor_has_customer_area,1,0) +
  IF
    (vendor_has_time_condition,1,0) +
  IF
    (vendor_has_customer_condition_adjusted,1,0) +
  IF
    (vendor_has_priority_delivery, 1,0) AS exposed_price_mechanism_count
    ---- APPLIED
    ,
  IF
    (is_dbdf, 1,0) +
  IF
    (is_dbmov, 1,0) +
  IF
    (is_surge_mov, 1,0) +
  IF
    (is_small_order_fee_adjusted, 1,0) +
  IF
    (is_fleet_delay, 1,0) +
  IF
    (is_basket_value_deal_adjusted, 1,0) +
  IF
    (is_service_fee_adjusted, 1,0) +
  IF
    (is_tod,1,0) +
  IF
    (is_fdnc_adjusted,1,0) +
  IF
    (is_customer_location, 1,0) +
  IF
    (has_priority_delivery, 1,0) AS order_price_mechanism_count
  FROM
    add_manual_fixes )
  #####
  ##### FINAL SCHEMA
  ,
  final_table AS (
  SELECT
    region,
    management_entity,
    entity_id,
    country_name,
    city_name,
    vertical_parent,
    vertical_type,
    is_own_delivery,
    vendor_id,
    init_week,
    created_date_local,
    order_placed_at,
    platform_order_code,
    scheme_id,
    vendor_price_scheme_type,
    assignment_id,
    delivery_fee_local,
    service_fee_local,
    mov_customer_fee_local,
    delivery_fee_eur,
    service_fee_eur
    ---- DPS + LOCAL
    ,
    STRUCT( vendor_asa_price_mechanism_count,
      exposed_price_mechanism_count,
      order_price_mechanism_count ) price_mechanism_fields,
    STRUCT(vendor_has_dbdf,
      vendor_has_dbmov,
      vendor_has_fleet_delay,
      vendor_has_surge_mov,
      vendor_has_basket_value_deal_adjusted AS vendor_has_basket_value_deal,
      vendor_has_time_condition,
      vendor_has_customer_area,
      vendor_has_customer_condition_adjusted AS vendor_has_customer_condition,
      vendor_has_service_fee_adjusted AS vendor_has_service_fee,
      vendor_has_small_order_fee_adjusted AS vendor_has_small_order_fee,
      IFNULL(vendor_has_priority_delivery, FALSE) AS vendor_has_priority_delivery ) AS vendor_price_mechanisms,
    STRUCT(is_dbdf,
      is_dbmov,
      is_surge_mov,
      is_fleet_delay,
      is_basket_value_deal_adjusted AS is_basket_value_deal,
      is_tod,
      is_customer_location,
      is_service_fee_adjusted AS is_service_fee,
      is_small_order_fee_adjusted AS is_small_order_fee,
      is_fdnc_adjusted AS is_fdnc,
      IFNULL(has_priority_delivery, FALSE) AS is_priority_delivery ) AS order_price_mechanisms
    ---- ONLY DPS
    ,
    STRUCT( vendor_asa_price_mechanism_count_only_dps AS vendor_asa_price_mechanism_count,
      exposed_price_mechanism_count_only_dps AS exposed_price_mechanism_count,
      order_price_mechanism_count_only_dps AS order_price_mechanism_count ) only_dps_price_mechanisms,
    STRUCT(vendor_has_dbdf,
      vendor_has_dbmov,
      vendor_has_fleet_delay,
      vendor_has_surge_mov,
      vendor_has_basket_value_deal,
      vendor_has_time_condition,
      vendor_has_customer_area,
      vendor_has_customer_condition,
      vendor_has_service_fee,
      vendor_has_small_order_fee,
      FALSE AS vendor_has_priority_delivery ) AS only_dps_vendor_price_mechanisms,
    STRUCT(is_dbdf,
      is_dbmov,
      is_surge_mov,
      is_fleet_delay,
      is_basket_value_deal,
      is_tod,
      is_customer_location,
      is_service_fee,
      is_small_order_fee,
      is_fdnc,
      FALSE AS is_priority_delivery,
      mov_type,
      service_fee_type ) AS only_dps_scheme_price_mechanisms,
    STRUCT( with_customer_condition,
      asa_customer_condition_schemes,
      test_customer_condition_schemes
      -- , fdnc_order_qty
      ,
      vendor_has_customer_condition_in_campaign ) AS customer_condition_source_fields,
    STRUCT(with_time_condition,
      asa_time_condition_schemes,
      test_time_condition_schemes ) AS time_condition_source_fields,
    STRUCT( with_customer_area_condition,
      asa_customer_location_schemes,
      test_customer_location_schemes ) AS customer_area_condition_source_fields,
    vendor_price_variant
  FROM
    add_number_of_mechanisms )
SELECT
  *
FROM
  final_table
  -- FROM load_vendor_price_data
WHERE
  TRUE
  -- AND entity_id = "FP_TW"
  -- and vendor_id = "psym"
  -- AND init_week >= ""
  ;
  #####
  ##################################################################
  ################################################################## UPSERT
IF
  backfill THEN
CREATE OR REPLACE TABLE
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd`
  -- DROP TABLE IF EXISTS `logistics-data-storage-staging.temp_pricing.sl_pricing_mechanism_data_dev`;
  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sl_pricing_mechanism_data_dev`
PARTITION BY
  created_date_local
CLUSTER BY
  entity_id,
  platform_order_code OPTIONS ( description = "This table keeps the information related to DPS price mechanisms usage of an order at an order level." ) AS
SELECT
  *
FROM
  staging_data;
  ELSE
MERGE INTO
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` prd
  -- MERGE INTO `dh-logistics-product-ops.pricing.sl_pricing_mechanism_data_dev` prd
USING
  staging_data stg
ON
  prd.platform_order_code = stg.platform_order_code
  AND prd.entity_id = stg.entity_id
  WHEN MATCHED THEN UPDATE SET prd.region = stg.region, prd.management_entity = stg.management_entity, prd.entity_id = stg.entity_id, prd.country_name = stg.country_name, prd.city_name = stg.city_name, prd.vertical_parent = stg.vertical_parent, prd.vertical_type = stg.vertical_type, prd.is_own_delivery = stg.is_own_delivery, prd.vendor_id = stg.vendor_id, prd.init_week = stg.init_week, prd.created_date_local = stg.created_date_local, prd.order_placed_at = stg.order_placed_at, prd.platform_order_code = stg.platform_order_code, prd.scheme_id = stg.scheme_id, prd.vendor_price_scheme_type = stg.vendor_price_scheme_type, prd.assignment_id = stg.assignment_id, prd.delivery_fee_local = stg.delivery_fee_local, prd.service_fee_local = stg.service_fee_local, prd.mov_customer_fee_local = stg.mov_customer_fee_local, prd.delivery_fee_eur = stg.delivery_fee_eur, prd.service_fee_eur = stg.service_fee_eur, prd.price_mechanism_fields = stg.price_mechanism_fields, prd.vendor_price_mechanisms = stg.vendor_price_mechanisms, prd.order_price_mechanisms = stg.order_price_mechanisms, prd.only_dps_price_mechanisms = stg.only_dps_price_mechanisms, prd.only_dps_vendor_price_mechanisms = stg.only_dps_vendor_price_mechanisms, prd.only_dps_scheme_price_mechanisms = stg.only_dps_scheme_price_mechanisms, prd.customer_condition_source_fields = stg.customer_condition_source_fields, prd.time_condition_source_fields = stg.time_condition_source_fields, prd.customer_area_condition_source_fields = stg.customer_area_condition_source_fields, prd.vendor_price_variant = stg.vendor_price_variant
  WHEN NOT MATCHED
  THEN
INSERT
  ROW ;
END IF
  ;
  ##################################################################
  ##################################################################    REPORTS
  ######################################## PM per Assignment
CREATE OR REPLACE TABLE
  `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_agg_data_per_assignment` AS
  -- create or replace table `dh-logistics-product-ops.pricing.pricing_mechanism_agg_data_per_assignment_dev` AS
SELECT
  p.init_week,
  p.region,
  p.management_entity,
  p.entity_id,
  p.city_name,
  p.vertical_parent,
  vertical_type,
  p.vendor_price_scheme_type,
  p.is_own_delivery,
  p.assignment_id,
  COUNT(platform_order_code) AS order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_dbdf THEN platform_order_code
  END
    ) AS dbdf_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_fleet_delay THEN platform_order_code
  END
    ) AS surge_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_basket_value_deal THEN platform_order_code
  END
    ) AS basket_value_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_service_fee THEN platform_order_code
  END
    ) AS service_fee_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_small_order_fee THEN platform_order_code
  END
    ) AS small_order_fee_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_dbmov THEN platform_order_code
  END
    ) AS variable_mov_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_surge_mov THEN platform_order_code
  END
    ) AS surge_mov_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_priority_delivery THEN platform_order_code
  END
    ) AS priority_delivery_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code
  END
    ) AS tod_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code
  END
    ) AS fdnc_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code
  END
    ) AS customer_location_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count >= 4 THEN platform_order_code
  END
    ) AS multiple_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 3 THEN platform_order_code
  END
    ) AS triple_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 2 THEN platform_order_code
  END
    ) AS double_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 1 THEN platform_order_code
  END
    ) AS single_pm_order_qty,
  SUM(only_dps_price_mechanisms.exposed_price_mechanism_count) AS pricing_mechanisms,
  SUM(delivery_fee_local) sum_delivery_fee_local,
  SUM(delivery_fee_eur) sum_delivery_fee_eur,
  SUM(service_fee_local) sum_service_fee_local,
  SUM(service_fee_eur) sum_service_fee_eur,
FROM
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
WHERE
  init_week >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK), INTERVAL 8 WEEK)
  AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10;
  ########################################\
  ######################################## ASA PM DATA
CREATE OR REPLACE TABLE
  `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_asa_agg_data` AS
SELECT
  p.init_week,
  p.region,
  p.management_entity,
  p.entity_id,
  p.city_name,
  p.vertical_parent,
  vertical_type,
  p.is_own_delivery,
  "DPS + Local" AS measure_type,
  COUNT(platform_order_code) AS order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_dbdf THEN platform_order_code
  END
    ) AS dbdf_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_fleet_delay THEN platform_order_code
  END
    ) AS surge_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_basket_value_deal THEN platform_order_code
  END
    ) AS basket_value_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_service_fee THEN platform_order_code
  END
    ) AS service_fee_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_small_order_fee THEN platform_order_code
  END
    ) AS small_order_fee_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_dbmov THEN platform_order_code
  END
    ) AS variable_mov_order_qty,
  COUNT(CASE
      WHEN order_price_mechanisms.is_surge_mov THEN platform_order_code
  END
    ) AS surge_mov_order_qty,
  COUNT(CASE
      WHEN vendor_price_mechanisms.vendor_has_priority_delivery THEN platform_order_code
  END
    ) AS priority_delivery_order_qty,
  COUNT(CASE
      WHEN vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code
  END
    ) AS tod_order_qty,
  COUNT(CASE
      WHEN vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code
  END
    ) AS fdnc_order_qty,
  COUNT(CASE
      WHEN vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code
  END
    ) AS customer_location_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" THEN platform_order_code
  END
    ) AS flat_mov_order_qty,
  COUNT(CASE
      WHEN price_mechanism_fields.exposed_price_mechanism_count >= 4 THEN platform_order_code
  END
    ) AS multiple_pm_order_qty,
  COUNT(CASE
      WHEN price_mechanism_fields.exposed_price_mechanism_count = 3 THEN platform_order_code
  END
    ) AS triple_pm_order_qty,
  COUNT(CASE
      WHEN price_mechanism_fields.exposed_price_mechanism_count = 2 THEN platform_order_code
  END
    ) AS double_pm_order_qty,
  COUNT(CASE
      WHEN price_mechanism_fields.exposed_price_mechanism_count = 1 THEN platform_order_code
  END
    ) AS single_pm_order_qty,
  SUM(price_mechanism_fields.exposed_price_mechanism_count) AS pricing_mechanisms,
  SUM(delivery_fee_local) sum_delivery_fee_local,
  SUM(delivery_fee_eur) sum_delivery_fee_eur,
  SUM(service_fee_local) sum_service_fee_local,
  SUM(service_fee_eur) sum_service_fee_eur,
FROM
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
WHERE
  vendor_price_scheme_type <> "Campaign"
  AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9
UNION ALL
SELECT
  p.init_week,
  p.region,
  p.management_entity,
  p.entity_id,
  p.city_name,
  p.vertical_parent,
  vertical_type,
  p.is_own_delivery,
  "DPS" AS measure_type,
  COUNT(platform_order_code) AS order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_dbdf THEN platform_order_code
  END
    ) AS dbdf_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_fleet_delay THEN platform_order_code
  END
    ) AS surge_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_basket_value_deal THEN platform_order_code
  END
    ) AS basket_value_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_service_fee THEN platform_order_code
  END
    ) AS service_fee_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_small_order_fee THEN platform_order_code
  END
    ) AS small_order_fee_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_dbmov THEN platform_order_code
  END
    ) AS variable_mov_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.is_surge_mov THEN platform_order_code
  END
    ) AS surge_mov_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_priority_delivery THEN platform_order_code
  END
    ) AS priority_delivery_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code
  END
    ) AS tod_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code
  END
    ) AS fdnc_order_qty,
  COUNT(CASE
      WHEN only_dps_vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code
  END
    ) AS customer_location_order_qty,
  COUNT(CASE
      WHEN only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" THEN platform_order_code
  END
    ) AS flat_mov_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count >= 4 THEN platform_order_code
  END
    ) AS multiple_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 3 THEN platform_order_code
  END
    ) AS triple_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 2 THEN platform_order_code
  END
    ) AS double_pm_order_qty,
  COUNT(CASE
      WHEN only_dps_price_mechanisms.exposed_price_mechanism_count = 1 THEN platform_order_code
  END
    ) AS single_pm_order_qty,
  SUM(only_dps_price_mechanisms.exposed_price_mechanism_count) AS pricing_mechanisms,
  SUM(delivery_fee_local) sum_delivery_fee_local,
  SUM(delivery_fee_eur) sum_delivery_fee_eur,
  SUM(service_fee_local) sum_service_fee_local,
  SUM(service_fee_eur) sum_service_fee_eur,
FROM
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
WHERE
  vendor_price_scheme_type <> "Campaign"
  AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9;
  ########################################
  ######################################## EXPERIMENT IMPACT
CREATE OR REPLACE TABLE
  `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_experiment_impact` AS
SELECT
  init_week,
  region,
  management_entity,
  entity_id,
  vertical_parent,
  vertical_type,
  is_own_delivery,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" THEN platform_order_code
  END
    ) AS order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_dbdf THEN platform_order_code
  END
    ) AS dbdf_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_fleet_delay THEN platform_order_code
  END
    ) AS surge_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_basket_value_deal THEN platform_order_code
  END
    ) AS basket_value_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_service_fee THEN platform_order_code
  END
    ) AS service_fee_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_small_order_fee THEN platform_order_code
  END
    ) AS small_order_fee_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_dbmov THEN platform_order_code
  END
    ) AS variable_mov_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_surge_mov THEN platform_order_code
  END
    ) AS surge_mov_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_priority_delivery THEN platform_order_code
  END
    ) AS priority_delivery_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code
  END
    ) AS tod_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code
  END
    ) AS fdnc_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code
  END
    ) AS customer_location_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 4 THEN platform_order_code
  END
    ) AS multiple_pm_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 3 THEN platform_order_code
  END
    ) AS triple_pm_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 2 THEN platform_order_code
  END
    ) AS double_pm_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 1 THEN platform_order_code
  END
    ) AS single_pm_order_qty,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") THEN platform_order_code
  END
    ) AS order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_dbdf THEN platform_order_code
  END
    ) AS dbdf_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_fleet_delay THEN platform_order_code
  END
    ) AS surge_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_basket_value_deal THEN platform_order_code
  END
    ) AS basket_value_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_service_fee THEN platform_order_code
  END
    ) AS service_fee_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_small_order_fee THEN platform_order_code
  END
    ) AS small_order_fee_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_dbmov THEN platform_order_code
  END
    ) AS variable_mov_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_surge_mov THEN platform_order_code
  END
    ) AS surge_mov_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_priority_delivery THEN platform_order_code
  END
    ) AS priority_delivery_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code
  END
    ) AS tod_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code
  END
    ) AS fdnc_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code
  END
    ) AS customer_location_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 4 THEN platform_order_code
  END
    ) AS multiple_pm_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 3 THEN platform_order_code
  END
    ) AS triple_pm_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 2 THEN platform_order_code
  END
    ) AS double_pm_order_qty_no_experiment,
  COUNT(CASE
      WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 1 THEN platform_order_code
  END
    ) AS single_pm_order_qty_no_experiment
FROM
  `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd`
WHERE
  init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7
  ########################################
  ##################################################################