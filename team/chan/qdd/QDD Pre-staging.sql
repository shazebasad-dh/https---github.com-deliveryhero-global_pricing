########## DECLARE VARIABLES

DECLARE start_date_filter, end_date_filter DATE;
DECLARE backfill BOOL;


########## SET RUN MODE
SET backfill = FALSE;

# SET END DATE 
SET end_date_filter = CURRENT_DATE();


# SET PARTITION DATE
IF backfill THEN 
    SET start_date_filter = DATE_SUB("2022-01-01", interval 7 DAY); 
ELSE
    SET start_date_filter = DATE_SUB(end_date_filter, interval 30 DAY);
END IF; 


############ CREATE STAGING TABLE
create temp table staging_table AS

   WITH average_commission AS(
        SELECT
        country_code,
        vendor_id,
        avg(gfv_local) avg_gfv,
        avg(commission_local) avg_commission
        FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
        WHERE 1=1
        AND created_date BETWEEN current_date()-84 AND current_date()-2
        AND commission_local IS NOT NULL
        AND commission_local <> 0
        AND gfv_local IS NOT NULL
        AND gfv_local <> 0
        GROUP BY 1,2
    )
    
    , commission_percentage AS(
        SELECT
        country_code,
        vendor_id,
        safe_divide(avg_commission, avg_gfv) commission_percentage
        FROM average_commission
    )


  , load_fct_info as (
    SELECT
      CAST(order_id AS string) as platform_order_code
      , is_tpro_order
      , app_version
      , lower(country_Code) as country_code
      , delivery_arrangement = 'PICKUP' as is_pickup

    FROM `tlb-data-prod.data_platform.fct_order_info`
    where order_date BETWEEN date_sub(start_date_filter, interval 2 day) AND date_add(end_date_filter, interval 2 day)

  )

    , load_dps_sessions as (
    select
    platform_order_code
    , country_code
    , vendor_price_scheme_type
    , dps_small_order_fee_local
    , scheme_id
    , assignment_id
    , dps_mean_delay

    from `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders_v2`
    where created_date BETWEEN date_sub(start_date_filter, interval 2 day) AND date_add(end_date_filter, interval 2 day)
    )
    
    , real_df AS(
        SELECT DISTINCT
        map.created_date_local
        , map.test_name
        , map.variant
        , map.order_id
        , vertical_type
        , map.entity_id
        , map.platform_order_code
        , delivery_fee_local
        , map.dps_delivery_fee_local
        , map.dps_surge_fee_local
        , map.dps_minimum_order_value_local
        , dps_small_order_fee_local
        , map.gfv_local
        , CAST(map.gfv_eur AS FLOAT64) gfv_eur
        , CAST(map.delivery_costs_local AS FLOAT64) delivery_costs_local
        , CAST(map.to_customer_time AS FLOAT64) AS to_customer_time
        , CAST(map.travel_time AS FLOAT64) AS travel_time
        , CAST(map.delivery_distance AS FLOAT64) AS delivery_distance
        , CAST(map.mean_delay AS FLOAT64) AS mean_delay
        , CAST(map.service_fee_local AS FLOAT64) AS service_fee_local
        , map.created_date
        , map.country_code
        , target_group
        , dps.vendor_price_scheme_type
        , vat_ratio
        , is_tpro_order as is_subscription_order
        , scheme_id
        , assignment_id

        ---- Commission proxy
        , CASE
              WHEN map.commission_local IS NULL THEN gfv_local * cm.commission_percentage
              WHEN map.commission_local = 0 THEN gfv_local * cm.commission_percentage
              ELSE map.commission_local 
          END AS commission_local

        ---- FIX TB DQI
          , CASE 
              WHEN (dps_small_order_fee_local is null OR dps_small_order_fee_local = 0) AND mov_customer_fee_local > 0 then 0
              WHEN mov_customer_fee_local > 0 AND tb.is_tpro_order then 0 
              WHEN mov_customer_fee_local > 0 AND tb.is_pickup then 0
              WHEN mov_customer_fee_local > 0 AND map.operating_system = "Android" and tb.app_version <= "9.6.0" then 0
              WHEN mov_customer_fee_local > 0 AND map.operating_system = "iOS" and tb.app_version <= "9.4.8" then 0
              WHEN mov_customer_fee_local > 0 AND map.operating_system in ("Web", "mWeb") then 0
              ELSE mov_customer_fee_local 
          END as mov_customer_fee_local

        
        
        FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` map

        LEFT JOIN commission_percentage cm    
          ON map.vendor_id = cm.vendor_id
          and map.country_code = cm.country_code

        LEFT JOIN load_dps_sessions dps
          ON map.platform_order_code = dps.platform_order_code 
          AND map.country_code = dps.country_code

        LEFT JOIN load_fct_info tb
          ON map.platform_order_code = tb.platform_order_code
          AND map.country_code = tb.country_code

        INNER JOIN `fulfillment-dwh-production.cl.dps_experiment_setups` test
          ON map.test_name = test.test_name 
          AND test.misconfigured = FALSE

        WHERE 1=1
        AND variant NOT IN ('Original') 
        AND variant IS NOT NULL
        AND map.created_date BETWEEN start_date_filter AND end_date_filter
        AND delivery_distance IS NOT NULL
        AND travel_time IS NOT NULL    
        AND is_own_delivery
        AND vendor_price_scheme_type is not null
        )

        , add_revenue as (

          -- Added in Jan 2023

          SELECT *
            , ifnull(commission_local,0) 
              + ifnull(mov_customer_fee_local, 0)
              + ifnull(delivery_fee_local, 0)
              + ifnull(service_fee_local,0)
            AS revenue_local

          , SAFE_DIVIDE(
              ifnull(commission_local,0) 
              + ifnull(mov_customer_fee_local, 0)
              + ifnull(delivery_fee_local, 0)
              + ifnull(service_fee_local,0)
              , (1+vat_ratio)
            ) as revenue_no_vat_local

          , SAFE_DIVIDE(delivery_fee_local , (1+vat_ratio)) AS df_no_vat_local
          , SAFE_DIVIDE(commission_local, (1+vat_ratio)) as commission_no_vat_local

          --- ADD is_gfv_local below DPS

          , gfv_local < dps_minimum_order_value_local as is_gfv_below_mov
        
          FROM real_df

        )

    select
    *
    from add_revenue;


###### UPSERT
IF backfill THEN 
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.qdd_pre_staging_table`
  PARTITION BY created_date_local
  CLUSTER BY entity_id, test_name
  AS
  SELECT * FROM staging_table;
ELSE
  MERGE INTO `dh-logistics-product-ops.pricing.qdd_pre_staging_table` qdd
  USING staging_table stg
    ON qdd.platform_order_code = stg.platform_order_code
    AND qdd.country_code = stg.country_code
    AND qdd.test_name = stg.test_name
  WHEN MATCHED THEN
    UPDATE SET
        created_date_local = stg.created_date_local
        , test_name = stg.test_name
        , variant = stg.variant
        , order_id = stg.order_id
        , vertical_type = stg.vertical_type
        , entity_id = stg.entity_id
        , platform_order_code = stg.platform_order_code
        , delivery_fee_local = stg.delivery_fee_local
        , dps_delivery_fee_local = stg.dps_delivery_fee_local
        , dps_surge_fee_local = stg.dps_surge_fee_local
        , dps_minimum_order_value_local = stg.dps_minimum_order_value_local
        , commission_local = stg.commission_local
        , mov_customer_fee_local = stg.mov_customer_fee_local
        , dps_small_order_fee_local = stg.dps_small_order_fee_local
        , gfv_local = stg.gfv_local
        , gfv_eur = stg.gfv_eur
        , delivery_costs_local = stg.delivery_costs_local
        , to_customer_time = stg.to_customer_time
        , travel_time = stg.travel_time
        , delivery_distance = stg.delivery_distance
        , mean_delay = stg.mean_delay
        , created_date = stg.created_date
        , country_code = stg.country_code
        , service_fee_local = stg.service_fee_local
        , target_group = stg.target_group
        , vendor_price_scheme_type = stg.vendor_price_scheme_type
        , vat_ratio = stg.vat_ratio
        , revenue_local = stg.revenue_local
        , revenue_no_vat_local = stg.revenue_no_vat_local
        , df_no_vat_local = stg.df_no_vat_local
        , commission_no_vat_local = stg.commission_no_vat_local
        , is_subscription_order = stg.is_subscription_order
        , scheme_id = stg.scheme_id
        , assignment_id = stg.assignment_id
        , is_gfv_below_mov = stg.is_gfv_below_mov

  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
end if;