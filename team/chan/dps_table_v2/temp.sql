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
      SET date_partition = DATE_SUB("2022-01-01", interval 1 WEEK); 
      -- SET date_partition = DATE_SUB(current_week, interval 4 WEEK); 
  ELSE
      SET date_partition = DATE_SUB(current_week, interval 5 WEEK);
  END IF; 

  # PRINT INPUTS
  SELECT
  backfill as backfill
  , date_partition as from_date
  , current_week as to_date
  ;

##################################################

################################################## CREATE STAGING TABLE

  CREATE OR REPLACE TEMP TABLE  staging_data AS
  ##### LOAD DIMENSIONS

    with countries as (

        select distinct
        segment as region
        , management_entity
        , country_name
        , global_entity_id as entity_id
        from  `fulfillment-dwh-production.curated_data_shared_coredata.global_entities`
    )

    , load_vendors_parent_vertical as (
      select 
      vertical_parent
      , vendor_id
      , global_entity_id as entity_id
        from `fulfillment-dwh-production.curated_data_shared_coredata_business.vendors`
    )

  #####
  ##### LOAD PM Data

    , load_vendor_price_data as (
      SELECT entity_id
      /*
      Includes service fee overrides
      */
        , vendor_code as vendor_id
        , active_from
        , IFNULL(active_to, CURRENT_TIMESTAMP) as active_to
        , vendor_price_mechanisms.vendor_has_dbdf
        , vendor_price_mechanisms.vendor_has_dbmov
        , vendor_price_mechanisms.vendor_has_fleet_delay
        , vendor_price_mechanisms.vendor_has_small_order_fee
        , vendor_price_mechanisms.vendor_has_surge_mov
        , vendor_price_mechanisms.vendor_has_basket_value_deal
        , vendor_price_mechanisms.vendor_has_service_fee
        , vendor_price_mechanisms.vendor_has_time_condition
        , vendor_price_mechanisms.vendor_has_customer_condition
        , vendor_price_mechanisms.vendor_has_customer_area
      FROM `logistics-data-storage-staging.long_term_pricing.vendor_full_pricing_configuration_versions`
    )

  ,  load_fdnc_vendors as (
      SELECT DISTINCT
      entity_id
      , vendor_code
      , fdnc_valid_from
      , IFNULL(fdnc_valid_to, CURRENT_TIMESTAMP()) fdnc_valid_to
      , TRUE as vendor_has_customer_condition_in_campaign
      FROM `logistics-data-storage-staging.long_term_pricing.vendor_customer_condition_campaign_versions`
  )

  -- , clean_fdnc_vendors as (
  --   SELECT DISTINCT 
  --   entity_id
  --   , vendor_code
  --   , active_from
  --   , fdnc_active_to
  --   , TRUE as vendor_has_customer_condition_in_campaign
  --   from load_fdnc_vendors
  -- )

    , load_asa_condition_data as (
        SELECT 
          entity_id
          , asa_id
          , active_from
          , IFNULL(active_to,CURRENT_TIMESTAMP) active_to
          , ARRAY(
              SELECT DISTINCT 
              scheme_id
              FROM UNNEST(asa_price_config) apc
              WHERE apc.schedule_id IS NOT NULL
          ) as asa_time_condition_schemes

          , ARRAY(
              SELECT DISTINCT 
              scheme_id
              FROM UNNEST(asa_price_config) apc
              WHERE apc.customer_condition_id IS NOT NULL
          ) as asa_customer_condition_schemes

          , ARRAY(
              SELECT DISTINCT 
              scheme_id
              FROM UNNEST(asa_price_config) apc
              WHERE apc.n_areas > 0
          ) as asa_customer_location_schemes
        FROM  `fulfillment-dwh-production.cl._pricing_asa_configuration_versions`
    )

    , load_exp_condition_data as (
        SELECT
        entity_id
        , test_id
        , variation_group
        , ARRAY_AGG(
          STRUCT(
            price_scheme_id AS scheme_id
          , customer_condition.id AS customer_condition_id
          , schedule.id as schedule_id
          , ARRAY_LENGTH(customer_areas) as n_areas
          )
        ) AS test_price_config
        FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
        GROUP BY 1,2,3
    )
    

    , experiment_condition_clean as (

      SELECT 
      entity_id
      , test_id
      , variation_group as vendor_price_variant
      , ARRAY(
          SELECT DISTINCT 
          scheme_id
          FROM UNNEST(test_price_config) apc
          WHERE apc.schedule_id IS NOT NULL
      ) as test_time_condition_schemes

      , ARRAY(
          SELECT DISTINCT 
          scheme_id
          FROM UNNEST(test_price_config) apc
          WHERE apc.customer_condition_id IS NOT NULL
      ) as test_customer_condition_schemes

      , ARRAY(
          SELECT DISTINCT 
          scheme_id
          FROM UNNEST(test_price_config) apc
          WHERE apc.n_areas > 0
      ) as test_customer_location_schemes

      FROM load_exp_condition_data
    )

    , add_test_has_condition_mechanisms as (
      SELECT *
        , ARRAY_LENGTH(test_time_condition_schemes) > 0 as test_has_time_condition
        , ARRAY_LENGTH(test_customer_condition_schemes) > 0 as test_has_customer_condition
        , ARRAY_LENGTH(test_customer_location_schemes) > 0 as test_has_customer_location
      FROM experiment_condition_clean
    )


    , load_scheme_data as (
        SELECT 
            entity_id
            , scheme_id
            , scheme_active_from
            , IFNULL(scheme_active_to, CURRENT_TIMESTAMP()) scheme_active_to
            , scheme_price_mechanisms.is_dbdf
            , scheme_price_mechanisms.is_dbmov
            , scheme_price_mechanisms.is_surge_mov
            , scheme_price_mechanisms.is_small_order_fee
            , scheme_price_mechanisms.is_fleet_delay
            , scheme_price_mechanisms.is_basket_value_deal
            , scheme_price_mechanisms.is_service_fee
            , scheme_price_mechanisms.mov_type
            , scheme_price_mechanisms.service_fee_type
        FROM `fulfillment-dwh-production.cl.pricing_configuration_versions`
    )

    -- , load_bvd_hs_data as (
    --   select
    --   CAST(platform_order_code AS STRING) as platform_order_code
    --   , rdf_offer_applied as is_rdf_order
    --   from `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders`
    --   where operation_day >= date_partition  - interval 2 WEEK
    --   )

    , load_prio_delivery as (
        SELECT DISTINCT
        global_order_id as platform_order_code
        , entity_id
        , TRUE as has_priority_delivery
        FROM `fulfillment-dwh-production.cl.orders_v2` 
        LEFT JOIN UNNEST(rider.tags) tags
        WHERE created_date >= date_partition  - interval 2 WEEK
        AND tags IN (
          'PRIORITIZE_DELIVERY' --peya
          , 'priority_delivery' --pandora
          )
    )

  #####
  ##### LOAD ORDER DATA AND JOIN PM DATA 

      , dps_order_raw as (
          select  entity_id,
          date_trunc(created_date_local, ISOWEEK) as init_week, -- Week start monday
          vertical_type,
          platform_order_code,
          created_date_local,
          order_placed_at,
          CAST(scheme_id AS INT64) as scheme_id,
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

          from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders`
          where created_date >= date_partition  - interval 1 WEEK
          AND created_date_local >=  date_partition
          AND created_date_local <= current_week
          AND is_sent
          AND scheme_id IS NOT NULL
      )

      , dps_order as (
          select
          *
          from dps_order_raw
          LEFT JOIN load_vendors_parent_vertical 
              using(vendor_id,entity_id)
      )

      , dps_order_with_pm as (
          SELECT 
          c.*  except(entity_id)
          , dps.*
          , s.* except(entity_id, vendor_id, active_from, active_to)
          , sch.* EXCEPT(entity_id, scheme_id, scheme_active_from, scheme_active_to)
          , asa.* EXCEPT(entity_id, asa_id, active_from, active_to)
          , exper.* EXCEPT(entity_id, test_id, vendor_price_variant)
        --   , hs.is_rdf_order
          , IFNULL(prio.has_priority_delivery, FALSE) as has_priority_delivery
          , IFNULL(fdnc.vendor_has_customer_condition_in_campaign,FALSE) as vendor_has_customer_condition_in_campaign

          from dps_order dps
          LEFT JOIN countries c
              using(entity_id)

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

        --   LEFT JOIN load_bvd_hs_data hs
        --     ON dps.entity_id = "HS_SA"
        --     AND dps.platform_order_code = hs.platform_order_code

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

      , weekly_prio_order as (
        /*
        Priority is enabled at the country level, so if a country has just one prio order 
        all orders must have the ability to be prio
        */
        SELECT DISTINCT
        init_week
        , entity_id
        , zone_id
        , is_own_delivery
        , has_priority_delivery as vendor_has_priority_delivery
        FROM dps_order_with_pm
        WHERE has_priority_delivery = TRUE
        AND is_own_delivery
      )

      , make_condition_mech_robust as (
        SELECT * EXCEPT(
          vendor_has_customer_condition
          , vendor_has_time_condition
          , vendor_has_customer_area
          , is_service_fee
        )

        , CASE
            WHEN vendor_has_customer_condition_in_campaign then TRUE
            WHEN vendor_price_scheme_type = "Experiment" then test_has_customer_condition
            ELSE vendor_has_customer_condition
          END as vendor_has_customer_condition

        , CASE
            WHEN vendor_price_scheme_type = "Experiment" then test_has_time_condition
            ELSE vendor_has_time_condition
        END as vendor_has_time_condition

        , CASE
            WHEN vendor_price_scheme_type = "Experiment" then test_has_customer_location
            ELSE vendor_has_customer_area
        END as vendor_has_customer_area
        

        , CASE
            WHEN with_customer_condition then TRUE
            --- for before Sep22, when with_customer_condition wasn't available. 
            /*
            IF the applied scheme is tied to an condition, then the condition
            must have been TRUE at that moment. 
            */
            WHEN scheme_id IN UNNEST(asa_customer_condition_schemes) THEN TRUE 
            WHEN scheme_id IN UNNEST(test_customer_condition_schemes) THEN TRUE
          ELSE FALSE
          END AS is_fdnc

        , CASE
            WHEN with_time_condition then TRUE
            --- for before Sep22, when with_time_condition wasn't available
            WHEN scheme_id IN UNNEST(asa_time_condition_schemes) THEN TRUE 
            WHEN scheme_id IN UNNEST(test_time_condition_schemes) THEN TRUE
          ELSE FALSE
          END AS is_tod

        , CASE
            WHEN with_customer_area_condition then TRUE
            WHEN scheme_id IN UNNEST(asa_customer_location_schemes) THEN TRUE 
            WHEN scheme_id IN UNNEST(test_customer_location_schemes) THEN TRUE
          ELSE FALSE
          END AS is_customer_location


        , CASE
            WHEN components.service_fee_id IS NOT NULL THEN TRUE 
            ELSE is_service_fee
          END AS is_service_fee

        


        FROM dps_order_with_pm
        -- LEFT JOIN count_weekly_fdnc
            -- USING(init_week, region, entity_id, vendor_id)  
        LEFT JOIN weekly_prio_order
            USING(init_week, entity_id, zone_id, is_own_delivery) 
        )

      /*
      The Following CTEs adjust PM to take into account
      local implementations:
      - Pandora's Top Up is considered as SBF
      - HungerStation's RDF is a Basket Value
      - Some countries in Europe implements Service Fee outside DPS
      */
      
    --   , weekly_rdf_vendors as (
    --       SELECT DISTINCT
    --       init_week
    --       , entity_id
    --       , vendor_id
    --       , TRUE as is_rdf_vendor
    --       FROM dps_order_with_pm
    --       WHERE is_rdf_order = 1

    --       GROUP BY 1,2,3
    --   )

      , add_manual_fixes as (
      SELECT *
        ------ VENDOR CONFIGURATION

          ----- FDNC

            , CASE 
                when entity_id = "HS_SA" then TRUE 
                ELSE vendor_has_customer_condition
            END as vendor_has_customer_condition_adjusted

          ----- Service Fee

            ,  CASE 
                WHEN country_name in ("Sweden", "Norway", "Finland", "Czech Republic") then TRUE
                WHEN country_name = "Austria" and order_placed_at >= "2022-09-22" then TRUE
                WHEN entity_id IN ("FP_SK", "NP_HU") and order_placed_at>= "2023-01-01" then TRUE
                ELSE vendor_has_service_fee
            END as vendor_has_service_fee_adjusted

          ----- SOF

            , CASE 
                WHEN region = "Europe" AND entity_id NOT IN ('NP_HU', "FY_CY", "EF_GR") then TRUE
                WHEN region = "Asia" then TRUE
                else vendor_has_small_order_fee
            END AS vendor_has_small_order_fee_adjusted

          ----- BASKET VALUE DEALS 
          /*
          RDF offers in HS are considered BVD
          */
            , CASE 
                WHEN entity_id = "HS_SA" THEN TRUE
                ELSE vendor_has_basket_value_deal
            END AS vendor_has_basket_value_deal_adjusted


        ------ APPLIED MECHANISM

          , CASE 
              when entity_id = "HS_SA" then TRUE 
              ELSE is_fdnc
          END as is_fdnc_adjusted

          
          ----- SERVICE FEE

          ,  CASE 
                WHEN country_name in ("Sweden", "Norway", "Finland", "Czech Republic") then TRUE
                WHEN country_name = "Austria" and order_placed_at >= "2022-09-22" then TRUE
                WHEN entity_id IN ("FP_SK", "NP_HU") and order_placed_at>= "2023-01-01" then TRUE
                WHEN entity_id iN ("FY_CY") and order_placed_at>="2023-08-01" THEN TRUE
                ELSE is_service_fee
            END as is_service_fee_adjusted


            ----- SOF
            , CASE 
                WHEN region = "Europe" AND entity_id NOT IN ('NP_HU', "FY_CY", "EF_GR") then TRUE
                WHEN region = "Asia" then TRUE
                else is_small_order_fee
            END AS is_small_order_fee_adjusted

          ----- BASKET VALUE DEALS 
          /*
          RDF offers in HS are considered BVD
          */
            , CASE 
                WHEN entity_id = "HS_SA" THEN TRUE
                ELSE is_basket_value_deal
            END AS is_basket_value_deal_adjusted

        ------ APPLIED MECHANISM

          FROM make_condition_mech_robust
        --   LEFT JOIN weekly_rdf_vendors
        --     USING(init_week, entity_id, vendor_id)
          
      )

      , add_number_of_mechanisms as (
          select
          * 
        ---- ONLY DPS MECHANISMS FIELDS

          ---- CONFIGURED 
          , if(vendor_has_dbdf,1,0) 
              + if(vendor_has_time_condition,1,0)
              + if(vendor_has_fleet_delay,1,0) 
              + if(vendor_has_basket_value_deal, 1,0)
              + if(vendor_has_service_fee,1,0)
              + if(vendor_has_customer_condition,1,0)
              + if(vendor_has_small_order_fee,1,0)
              + if(vendor_has_dbmov, 1,0) 
              + if(vendor_has_surge_mov, 1,0) 
              + if(vendor_has_customer_area,1,0)
          AS vendor_asa_price_mechanism_count_only_dps

          ---- EXPOSED 
          , if(is_dbdf,1,0) 
              + if(is_fleet_delay,1,0) 
              + if(is_basket_value_deal, 1,0)
              + if(is_service_fee,1,0)
              + if(is_small_order_fee,1,0)
              + if(is_dbmov, 1,0) 
              + if(is_surge_mov, 1,0) 
              + if(vendor_has_customer_area,1,0)
              + if(vendor_has_time_condition,1,0)
              + if(vendor_has_customer_condition,1,0)
          AS exposed_price_mechanism_count_only_dps

          ---- APPLIED 
          , IF(is_dbdf, 1,0)
              + IF(is_dbmov, 1,0)
              + IF(is_surge_mov, 1,0)
              + IF(is_small_order_fee, 1,0)
              + IF(is_fleet_delay, 1,0)
              + IF(is_basket_value_deal, 1,0)
              + IF(is_service_fee, 1,0)
              + if(is_tod,1,0)
              + if(is_fdnc,1,0) 
              + if(is_customer_location, 1,0)
          AS order_price_mechanism_count_only_dps

        ----

        ---- ADJUSTED MECHANISMS
          ---- CONFIGURED 
          , if(vendor_has_dbdf,1,0) 
              + if(vendor_has_time_condition,1,0)
              + if(vendor_has_fleet_delay,1,0) 
              + if(vendor_has_basket_value_deal_adjusted, 1,0)
              + if(vendor_has_service_fee_adjusted,1,0)
              + if(vendor_has_customer_condition_adjusted,1,0)
              + if(vendor_has_small_order_fee_adjusted,1,0)
              + if(vendor_has_dbmov, 1,0) 
              + if(vendor_has_surge_mov, 1,0) 
              + if(vendor_has_customer_area,1,0)
              + if(vendor_has_priority_delivery, 1,0)
          AS vendor_asa_price_mechanism_count

          ---- EXPOSED 
          , if(is_dbdf,1,0) 
              + if(is_fleet_delay,1,0) 
              + if(is_basket_value_deal_adjusted, 1,0)
              + if(is_service_fee_adjusted,1,0)
              + if(is_small_order_fee_adjusted,1,0)
              + if(is_dbmov, 1,0) 
              + if(is_surge_mov, 1,0) 
              + if(vendor_has_customer_area,1,0)
              + if(vendor_has_time_condition,1,0)
              + if(vendor_has_customer_condition_adjusted,1,0)
              + if(vendor_has_priority_delivery, 1,0)
          AS exposed_price_mechanism_count

          ---- APPLIED 
          , IF(is_dbdf, 1,0)
              + IF(is_dbmov, 1,0)
              + IF(is_surge_mov, 1,0)
              + IF(is_small_order_fee_adjusted, 1,0)
              + IF(is_fleet_delay, 1,0)
              + IF(is_basket_value_deal_adjusted, 1,0)
              + IF(is_service_fee_adjusted, 1,0)
              + if(is_tod,1,0)
              + if(is_fdnc_adjusted,1,0) 
              + if(is_customer_location, 1,0)
              + if(has_priority_delivery, 1,0)

          AS order_price_mechanism_count
          from add_manual_fixes
      )
  #####
  ##### FINAL SCHEMA
      , final_table as (
        SELECT
        region
        , management_entity
        , entity_id
        , country_name
        , city_name
        , vertical_parent
        , vertical_type
        , is_own_delivery
        , vendor_id
        , init_week
        , created_date_local
        , order_placed_at
        , platform_order_code
        , scheme_id
        , vendor_price_scheme_type
        , assignment_id
        , delivery_fee_local
        , service_fee_local
        , mov_customer_fee_local
        , delivery_fee_eur
        , service_fee_eur

      ---- DPS + LOCAL
        , STRUCT(
            vendor_asa_price_mechanism_count
          , exposed_price_mechanism_count
          , order_price_mechanism_count
        ) price_mechanism_fields

        , STRUCT(vendor_has_dbdf
          , vendor_has_dbmov
          , vendor_has_fleet_delay
          , vendor_has_surge_mov
          , vendor_has_basket_value_deal_adjusted as vendor_has_basket_value_deal
          , vendor_has_time_condition
          , vendor_has_customer_area
          , vendor_has_customer_condition_adjusted AS vendor_has_customer_condition
          , vendor_has_service_fee_adjusted AS vendor_has_service_fee
          , vendor_has_small_order_fee_adjusted AS vendor_has_small_order_fee
          , IFNULL(vendor_has_priority_delivery, FALSE) as vendor_has_priority_delivery
        ) as vendor_price_mechanisms

        , STRUCT(is_dbdf
          , is_dbmov
          , is_surge_mov
          , is_fleet_delay
          , is_basket_value_deal_adjusted as is_basket_value_deal
          , is_tod
          , is_customer_location
          , is_service_fee_adjusted as is_service_fee
          , is_small_order_fee_adjusted as is_small_order_fee
          , is_fdnc_adjusted as is_fdnc
          , IFNULL(has_priority_delivery, FALSE) as is_priority_delivery
        ) as order_price_mechanisms

      ---- ONLY DPS
      
        , STRUCT(
            vendor_asa_price_mechanism_count_only_dps AS vendor_asa_price_mechanism_count
          , exposed_price_mechanism_count_only_dps AS exposed_price_mechanism_count
          , order_price_mechanism_count_only_dps AS order_price_mechanism_count
        ) only_dps_price_mechanisms

        , STRUCT(vendor_has_dbdf
          , vendor_has_dbmov
          , vendor_has_fleet_delay
          , vendor_has_surge_mov
          , vendor_has_basket_value_deal
          , vendor_has_time_condition
          , vendor_has_customer_area
          , vendor_has_customer_condition
          , vendor_has_service_fee
          , vendor_has_small_order_fee
          , FALSE as vendor_has_priority_delivery
        ) as only_dps_vendor_price_mechanisms

        , STRUCT(is_dbdf
          , is_dbmov
          , is_surge_mov
          , is_fleet_delay
          , is_basket_value_deal
          , is_tod
          , is_customer_location
          , is_service_fee
          , is_small_order_fee
          , is_fdnc
          , FALSE as is_priority_delivery
          , mov_type
          , service_fee_type
        ) as only_dps_scheme_price_mechanisms

        , STRUCT(
          with_customer_condition
          , asa_customer_condition_schemes
          , test_customer_condition_schemes
          -- , fdnc_order_qty
          , vendor_has_customer_condition_in_campaign
        ) as customer_condition_source_fields

        , STRUCT(with_time_condition
          , asa_time_condition_schemes
          , test_time_condition_schemes
        ) as time_condition_source_fields

        , STRUCT(
          with_customer_area_condition
          , asa_customer_location_schemes
          , test_customer_location_schemes
        ) as customer_area_condition_source_fields
        , vendor_price_variant

        FROM add_number_of_mechanisms
      )

      
      select *
      from final_table
      -- FROM load_vendor_price_data
      WHERE TRUE
      -- AND entity_id = "FP_TW"
      -- and vendor_id = "psym"
      -- AND init_week >= ""
      ;
  #####

##################################################################

################################################################## UPSERT


  IF backfill THEN 
    CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd`
    -- DROP TABLE IF EXISTS `logistics-data-storage-staging.temp_pricing.sl_pricing_mechanism_data_dev`;
    -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sl_pricing_mechanism_data_dev`
    PARTITION BY created_date_local
    CLUSTER BY entity_id, platform_order_code
    OPTIONS (
      description = "This table keeps the information related to DPS price mechanisms usage of an order at an order level."
      )
    AS
    SELECT * FROM staging_data;
  ELSE
    MERGE INTO `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` prd
    -- MERGE INTO `dh-logistics-product-ops.pricing.sl_pricing_mechanism_data_dev` prd
    USING staging_data stg
      ON prd.platform_order_code = stg.platform_order_code
      AND prd.entity_id = stg.entity_id
    WHEN MATCHED THEN
      UPDATE SET
    prd.region = stg.region
    , prd.management_entity = stg.management_entity
    , prd.entity_id = stg.entity_id
    , prd.country_name = stg.country_name
    , prd.city_name = stg.city_name
    , prd.vertical_parent = stg.vertical_parent
    , prd.vertical_type = stg.vertical_type
    , prd.is_own_delivery = stg.is_own_delivery
    , prd.vendor_id = stg.vendor_id
    , prd.init_week = stg.init_week
    , prd.created_date_local = stg.created_date_local
    , prd.order_placed_at = stg.order_placed_at
    , prd.platform_order_code = stg.platform_order_code
    , prd.scheme_id = stg.scheme_id
    , prd.vendor_price_scheme_type = stg.vendor_price_scheme_type
    , prd.assignment_id = stg.assignment_id
    , prd.delivery_fee_local = stg.delivery_fee_local
    , prd.service_fee_local = stg.service_fee_local
    , prd.mov_customer_fee_local = stg.mov_customer_fee_local
    , prd.delivery_fee_eur = stg.delivery_fee_eur
    , prd.service_fee_eur = stg.service_fee_eur
    , prd.price_mechanism_fields = stg.price_mechanism_fields
    , prd.vendor_price_mechanisms = stg.vendor_price_mechanisms
    , prd.order_price_mechanisms = stg.order_price_mechanisms
    , prd.only_dps_price_mechanisms = stg.only_dps_price_mechanisms
    , prd.only_dps_vendor_price_mechanisms = stg.only_dps_vendor_price_mechanisms
    , prd.only_dps_scheme_price_mechanisms = stg.only_dps_scheme_price_mechanisms
    , prd.customer_condition_source_fields = stg.customer_condition_source_fields
    , prd.time_condition_source_fields = stg.time_condition_source_fields
    , prd.customer_area_condition_source_fields = stg.customer_area_condition_source_fields
    , prd.vendor_price_variant = stg.vendor_price_variant

   WHEN NOT MATCHED THEN
      INSERT ROW
    ;
  end if;
##################################################################

##################################################################    REPORTS    


  ######################################## PM per Assignment

    create or replace table `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_agg_data_per_assignment` AS
    -- create or replace table `dh-logistics-product-ops.pricing.pricing_mechanism_agg_data_per_assignment_dev` AS
    select
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
      count(platform_order_code) as order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_order_qty,

      count(case when only_dps_vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end) as priority_delivery_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_order_qty,

      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 3 then platform_order_code end) as triple_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 2 then platform_order_code end) as double_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 1 then platform_order_code end) as single_pm_order_qty,
      sum(only_dps_price_mechanisms.exposed_price_mechanism_count) as pricing_mechanisms,
      sum(delivery_fee_local) sum_delivery_fee_local,
      sum(delivery_fee_eur) sum_delivery_fee_eur,
      sum(service_fee_local) sum_service_fee_local,
      sum(service_fee_eur) sum_service_fee_eur,
    from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
    where init_week >= DATE_SUB(DATE_TRUNC(current_date(), WEEK), INTERVAL 8 WEEK)
    AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
    group by 1,2,3,4,5,6,7,8,9,10;

  ########################################\
  
  ######################################## ASA PM DATA

    create or replace table `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_asa_agg_data` AS
    select
      p.init_week,
      p.region,
      p.management_entity,
      p.entity_id,
      p.city_name,
      p.vertical_parent,
      vertical_type,
      p.is_own_delivery,
      "DPS + Local" AS measure_type,
      count(platform_order_code) as order_qty,
      count(case when order_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_order_qty,
      count(case when order_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_order_qty,
      count(case when order_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_order_qty,
      count(case when order_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_order_qty,
      count(case when order_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_order_qty,
      count(case when order_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_order_qty,
      count(case when order_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_order_qty,
      
      count(case when vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end) as priority_delivery_order_qty,
      count(case when vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_order_qty,
      count(case when vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_order_qty,
      count(case when vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_order_qty,
      
      count(case when only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" then platform_order_code end) as flat_mov_order_qty,
      count(case when price_mechanism_fields.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_order_qty,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 3 then platform_order_code end) as triple_pm_order_qty,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 2 then platform_order_code end) as double_pm_order_qty,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 1 then platform_order_code end) as single_pm_order_qty,
      sum(price_mechanism_fields.exposed_price_mechanism_count) as pricing_mechanisms,
      sum(delivery_fee_local) sum_delivery_fee_local,
      sum(delivery_fee_eur) sum_delivery_fee_eur,
      sum(service_fee_local) sum_service_fee_local,
      sum(service_fee_eur) sum_service_fee_eur,
    from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
    where vendor_price_scheme_type <> "Campaign"
    AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)

    group by 1,2,3,4,5,6,7,8,9

    UNION ALL 

    select
      p.init_week,
      p.region,
      p.management_entity,
      p.entity_id,
      p.city_name,
      p.vertical_parent,
      vertical_type,
      p.is_own_delivery,
      "DPS" AS measure_type,
      count(platform_order_code) as order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_order_qty,
      count(case when only_dps_scheme_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_order_qty,

      count(case when only_dps_vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end) as priority_delivery_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_order_qty,
      count(case when only_dps_vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_order_qty,
      
      count(case when only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" then platform_order_code end) as flat_mov_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 3 then platform_order_code end) as triple_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 2 then platform_order_code end) as double_pm_order_qty,
      count(case when only_dps_price_mechanisms.exposed_price_mechanism_count = 1 then platform_order_code end) as single_pm_order_qty,
      sum(only_dps_price_mechanisms.exposed_price_mechanism_count) as pricing_mechanisms,
      sum(delivery_fee_local) sum_delivery_fee_local,
      sum(delivery_fee_eur) sum_delivery_fee_eur,
      sum(service_fee_local) sum_service_fee_local,
      sum(service_fee_eur) sum_service_fee_eur,
    from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
    where vendor_price_scheme_type <> "Campaign"
    AND init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
    group by 1,2,3,4,5,6,7,8,9;

  ########################################

  ######################################## EXPERIMENT IMPACT
    CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_mechanism_experiment_impact` AS
    SELECT
        init_week,
        region,
        management_entity,
        entity_id,
        vertical_parent,
        vertical_type,
        is_own_delivery,
        count(CASE WHEN vendor_price_scheme_type <> "Campaign" then platform_order_code END) as order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND order_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end) as priority_delivery_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 3 then platform_order_code end) as triple_pm_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 2 then platform_order_code end) as double_pm_order_qty,
        count(case when vendor_price_scheme_type <> "Campaign" AND price_mechanism_fields.exposed_price_mechanism_count >= 1 then platform_order_code end) as single_pm_order_qty,

        count(CASE WHEN vendor_price_scheme_type NOT IN ("Campaign", "Experiment") THEN platform_order_code END) as order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND order_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_priority_delivery then platform_order_code end) as priority_delivery_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 3 then platform_order_code end) as triple_pm_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 2 then platform_order_code end) as double_pm_order_qty_no_experiment,
        count(case when vendor_price_scheme_type NOT IN ("Campaign", "Experiment") AND price_mechanism_fields.exposed_price_mechanism_count >= 1 then platform_order_code end) as single_pm_order_qty_no_experiment
      FROM `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` 
      WHERE init_week < DATE_TRUNC(CURRENT_DATE(), WEEK)
      GROUP BY 1,2,3,4,5,6,7
  ########################################

##################################################################