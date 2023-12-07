########################################## INPUTS

  ########## DECLARE VARIABLES

  DECLARE start_date_filter, end_date_filter DATE;
  DECLARE backfill BOOL;


  ########## SET RUN MODE
  SET backfill = TRUE;

  # SET END DATE 
  SET end_date_filter = CURRENT_DATE();

  # SET PARTITION DATE
  IF backfill THEN 
      SET start_date_filter = DATE_SUB("2023-01-01", interval 0 DAY); 
  ELSE
      SET start_date_filter = DATE_TRUNC(DATE_SUB(end_date_filter, interval 1 MONTH), MONTH);
  END IF; 
##########################################

########################################## STAGING TABLE

  CREATE TEMP TABLE staging_table AS
  with 
    load_dps_prio_info AS (
      SELECT
      platform_order_code
        , entity_id
        , LOWER(country_code) AS country_code
        /*
        priority_fee_local is the transacted fee coming from order stream. If missing,
        we use the dps one
        */
        , IFNULL(priority_fee_local, dps_priority_fee_local) as priority_fee_local
      FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders`
      WHERE created_date >= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
        AND created_date < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
    )

    , load_priority_orders_from_cl AS (
      SELECT DISTINCT
        platform_order_code
        , entity.id as entity_id
        , DATE(order_placed_at, timezone) as created_date_local
        , TIMESTAMP(STRING(order_placed_at, timezone)) as created_at_local
        , 
      FROM `fulfillment-dwh-production.cl.orders` 
      LEFT JOIN UNNEST(tags) t
      WHERE 
        --- cl.orders parition columns
        created_date BETWEEN DATE_SUB(start_date_filter, INTERVAL 2 DAY) AND DATE_ADD(end_date_filter, INTERVAL 2 DAY)
        --- still filter by date at local time
        AND DATE(order_placed_at, timezone) BETWEEN start_date_filter AND end_date_filter
        AND order_status = "completed"
        AND t IN (
            'PRIORITIZE_DELIVERY' --peya
            , 'priority_delivery' --pandora
        )  
    )

    , load_peya_user_plus AS (
      SELECT
      CAST(order_id AS STRING) as order_id
      , LOWER(country.country_code) AS country_code
      , TRUE AS is_user_plus
      FROM `peya-bi-tools-pro.il_core.fact_orders`
      WHERE registered_date >= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
      AND registered_date < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
      AND is_user_plus = 1 -- we only need peya plus users
    )

    , add_prio_economics AS (
    SELECT 
      prio.*
      , dps.country_code
        /*
        Before 2023-11-13, PeYa implemented priority locally. 
        #todo improve this, this could be a mini table that we could join, will this improve performance?
        */
        , COALESCE(dps.priority_fee_local
          , CASE 
                WHEN created_date_local <= "2023-12-06" 
            THEN
              CASE
                  WHEN prio.entity_id = 'PY_AR' AND prio.created_date_local BETWEEN "2023-01-01" AND "2023-07-24" THEN 60
                  WHEN prio.entity_id = 'PY_AR' AND prio.created_date_local BETWEEN "2023-07-24" AND "2023-10-23" THEN 99
                  WHEN prio.entity_id = 'PY_AR' AND prio.created_date_local >= "2023-10-23" THEN 149 
                  WHEN prio.entity_id = 'PY_BO' THEN  4    
                  when prio.entity_id = 'PY_HN' then  10   
                  when prio.entity_id = 'PY_NI' then  12   
                  when prio.entity_id = 'AP_PA' then  0.25 
                  when prio.entity_id = 'PY_CR' then  200  
                  when prio.entity_id = 'PY_PY' then  3000 
                  when prio.entity_id = 'PY_SV' then  0.25 
                  when prio.entity_id = 'PY_CL' then  290  
                  when prio.entity_id = 'PY_PE' then  1    
                  when prio.entity_id = 'PY_EC' then  0.30 
                  when prio.entity_id = 'PY_UY' then  49   
              END 
            END
        ) AS priority_fee_local

    FROM load_dps_prio_info dps
    INNER JOIN load_priority_orders_from_cl prio
      ON dps.entity_id = prio.entity_id
      AND dps.platform_order_code = prio.platform_order_code
    )

    , add_peya_plus AS (
      /*
      PeYa Plus users don't pay Prio Fees
      */

      SELECT 
        src.* EXCEPT(priority_fee_local)
        , IF(is_user_plus IS NOT NULL, 0, priority_fee_local) as priority_fee_local
        , IFNULL(is_user_plus, FALSE) AS is_user_plus
      FROM add_prio_economics src
      LEFT JOIN load_peya_user_plus py
        ON src.country_code = py.country_code
        AND src.platform_order_code = py.order_id
    )

    , add_fx_rate AS (

    SELECT adp.*
    , SAFE_DIVIDE(priority_fee_local, fx_rate_eur) as priority_fee_eur
    FROM add_peya_plus adp
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ge
      ON adp.entity_id = ge.global_entity_id
    LEFT JOIN `fulfillment-dwh-production.curated_data_shared_coredata.fx_rates` fx
      ON ge.currency_iso_code = fx.currency_code
      AND adp.created_date_local = fx.fx_rate_date
    )

    SELECT 
    entity_id
    , country_code
    , created_date_local
    , created_at_local
    , platform_order_code
    , is_user_plus
    , priority_fee_local
    , priority_fee_eur
    FROM add_fx_rate;

##########################################

########################################## UPSERT
  IF backfill THEN 
    CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.prio_orders`
    PARTITION BY created_date_local
    OPTIONS(
      partition_expiration_days=null,
      require_partition_filter=false
    )
    AS
    SELECT * FROM staging_table;
  ELSE
    MERGE INTO `logistics-data-storage-staging.long_term_pricing.prio_orders` prd
    USING staging_table stg
      ON prd.entity_id = stg.entity_id
      AND prd.platform_order_code = stg.platform_order_code
    WHEN MATCHED THEN
      UPDATE SET
      entity_id = stg.entity_id
      , country_code = stg.country_code
      , created_date_local = stg.created_date_local
      , created_at_local = stg.created_at_local
      , platform_order_code = stg.platform_order_code
      , is_user_plus = stg.is_user_plus
      , priority_fee_local = stg.priority_fee_local
      , priority_fee_eur = stg.priority_fee_eur
    WHEN NOT MATCHED THEN
      INSERT ROW
    ;
  END IF;
##########################################