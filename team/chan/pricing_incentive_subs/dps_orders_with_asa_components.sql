###################### CONDITIONS FUNCTIONS 

  ###################### FDNC
    CREATE TEMP FUNCTION CHECK_CUSTOMER_CONDITION (
      vertical_type STRING
      , is_default_scheme BOOL
      , customer_condition_config STRUCT<
        description STRING
        , orders_number_less_than INT64
        , days_since_first_order_less_than INT64
        , counting_method STRING
      >
      , order_count_total NUMERIC
      , order_count_darkstores NUMERIC
      , order_count_qcommerce NUMERIC
      , days_since_first_order FLOAT64
      , days_since_first_darkstores_order FLOAT64
      , days_since_first_qcommerce_order FLOAT64
    ) RETURNS BOOL AS (
      -- just because the order count or first order date doens't match between DPS and the other IDs, it *doesn't* mean we accidentally granted free delivery.
      -- With multiple FDNC, the first 10 deliveries might be free for example. In this case, if we flag an order as order number 5 when it was in fact number 6, we didn't do anything wrong by granting FDNC. It's only at order number 10 that it's a problem.
      -- Therefore, we compare the order counts and first order dates reported by each of the identifiers to the maximum order count and max first order date in the ASA or Campaign configuration.
      CASE
        WHEN
          is_default_scheme IS TRUE
          OR (
            customer_condition_config.description IS NULL
            AND customer_condition_config.orders_number_less_than IS NULL
            AND customer_condition_config.days_since_first_order_less_than IS NULL
          )
          THEN TRUE -- Base case standard scheme / no condition check needed
        -- new to darkstore vertical conditions:
        WHEN (
          customer_condition_config.counting_method = 'VENDOR_VERTICAL' -- string used to identify new to vertical conditions
          AND vertical_type = 'darkstores' -- darkstores new customer conditions apply only to this vertical
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            order_count_darkstores <= customer_condition_config.orders_number_less_than
            AND days_since_first_darkstores_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'VENDOR_VERTICAL'
          AND vertical_type = 'darkstores'
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          ) THEN
            order_count_darkstores <= customer_condition_config.orders_number_less_than
        WHEN (
          customer_condition_config.counting_method = 'VENDOR_VERTICAL'
          AND vertical_type = 'darkstores'
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            days_since_first_darkstores_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'VENDOR_VERTICAL'
          AND vertical_type = 'darkstores'
          AND customer_condition_config.description = 'New'
          ) THEN
            order_count_darkstores = 0 OR order_count_darkstores IS NULL
        -- new to qcommerce vertical conditions:
        WHEN (
          customer_condition_config.counting_method = 'QCOMMERCE'
          AND vertical_type != 'restaurants' -- qcommerce orders are all orders not in restaurants or courier. Darkstore orders are a special qcommerce case handled above.
          AND vertical_type NOT LIKE 'courier%'
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            order_count_qcommerce <= customer_condition_config.orders_number_less_than
            AND days_since_first_qcommerce_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'QCOMMERCE'
          AND vertical_type != 'restaurants'
          AND vertical_type NOT LIKE 'courier%'
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          ) THEN
            order_count_qcommerce <= customer_condition_config.orders_number_less_than
        WHEN (
          customer_condition_config.counting_method = 'QCOMMERCE'
          AND vertical_type != 'restaurants'
          AND vertical_type NOT LIKE 'courier%'
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            days_since_first_qcommerce_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'QCOMMERCE'
          AND vertical_type != 'restaurants'
          AND vertical_type NOT LIKE 'courier%'
          AND customer_condition_config.description = 'New'
          ) THEN
            order_count_qcommerce = 0 OR order_count_qcommerce IS NULL
        -- regular new customer conditions:
        WHEN (
          customer_condition_config.counting_method = 'TOTAL'
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            order_count_total <= customer_condition_config.orders_number_less_than
            AND days_since_first_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'TOTAL'
          AND customer_condition_config.orders_number_less_than IS NOT NULL
          ) THEN
            order_count_total <= customer_condition_config.orders_number_less_than
        WHEN (
          customer_condition_config.counting_method = 'TOTAL'
          AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
          ) THEN
            days_since_first_order <= customer_condition_config.days_since_first_order_less_than
        WHEN (
          customer_condition_config.counting_method = 'TOTAL'
          AND customer_condition_config.description = 'New'
          ) THEN -- description can be New, Existing, or NULL
            order_count_total <= 1 OR order_count_total IS NULL -- It's the first order
      END
    );

  ######################

  ###################### TIME CONDITIONS
    -- CREATE OR REPLACE FUNCTION `logistics-data-storage-staging.temp_pricing.evaluate_dps_time_conditions` (
    CREATE TEMP FUNCTION  CHECK_TIME_CONDITIONS (
        order_placed_at TIMESTAMP    
          , schedule_start_at TIMESTAMP
          , schedule_end_at TIMESTAMP 
          , schedule_recurrence STRING
          , schedule_recurrence_end_at TIMESTAMP 
          , schedule_condition_end_at TIMESTAMP
          , schedule_active_days ARRAY<STRING>
          , schedule_is_all_day BOOL
      ) RETURNS BOOL
      

      AS  (
        /*
        IF there's no recurrence we just need to check the validity of the time condition.
        Once recurrence is involved, we need to check the validity of the recurrence window, based on two things:
        - The days the condition is active (day of week check)
        - The time window the condition is active (Time check)
        We do so by generating an date array with all the weekly dates using order_placed_at to define the week. 
        We contrast this weekly array with the time condition recurrence window. 
        IF a condition applies, there will be at least one date left within the array, thus, EXISTS returns TRUE. 
        Note that the comparison is SUNDAY-TO-SUNDAY. MONDAY-TO-SUNDAY will not capture conditions that apply just
        at the beginning of the week (e.g, 1:00 UTC)

        When schedule_is_all_day = TRUE, we need to ignore the time part of schedule_* inputs and check the whole day.

        This function ignores PRICE STICKINESS when used to check time conditions used in orders.
        */
        CASE 
          -- General check, the validity of the time conditions
          WHEN 
            CASE 
              WHEN schedule_is_all_day = TRUE 
                THEN order_placed_at >= TIMESTAMP_TRUNC(schedule_start_at, DAY) 
                  AND order_placed_at < IFNULL(TIMESTAMP_ADD(TIMESTAMP_TRUNC(schedule_condition_end_at, DAY), INTERVAL 1 DAY), "2099-01-01")
              ELSE order_placed_at BETWEEN schedule_start_at AND IFNULL(schedule_condition_end_at, "2099-01-01")
            END  
          THEN
            --- Recurrence check
            CASE
              WHEN schedule_recurrence = "NONE" THEN TRUE
              ELSE 
              EXISTS (
                SELECT
                condition_day
                FROM UNNEST(
                  GENERATE_DATE_ARRAY( 
                  DATE_SUB(DATE_TRUNC(DATE(order_placed_at), WEEK(MONDAY)), INTERVAL 1 DAY)
                  , DATE_ADD(DATE_SUB(DATE_TRUNC(DATE(order_placed_at), WEEK(MONDAY)), INTERVAL 1 DAY), INTERVAL 1 WEEK)
                  )
                ) condition_day
                WHERE TRUE
                --- DAY OF WEEK CHECK
                  AND IF(schedule_recurrence = "DAILY", TRUE, UPPER(FORMAT_DATE("%A", condition_day)) IN UNNEST(schedule_active_days))
                --- TIME CHECK
                  AND (
                    IF(schedule_is_all_day
                      , order_placed_at >= TIMESTAMP(condition_day) AND order_placed_at < TIMESTAMP_ADD(TIMESTAMP(condition_day), INTERVAL 1 DAY)
                      , DATETIME(order_placed_at) BETWEEN DATETIME(DATE(condition_day), EXTRACT(TIME FROM schedule_start_at))
                        AND DATETIME_ADD(DATETIME(DATE(condition_day), EXTRACT(TIME FROM schedule_start_at))
                        , INTERVAL TIMESTAMP_DIFF(schedule_end_at, schedule_start_at, MILLISECOND) MILLISECOND
                      )
                    )
                  )
              )
            END
          ELSE FALSE
        END
      )
    ;
  ######################

###################### 

###################### CREATE ORDER TABLE

  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sl_dps_sessions_with_asa_travel_times` AS

  with load_asa_config AS (
    SELECT entity_id
      , vendor_code
      , active_from
      , IFNULL(active_to, "2099-01-01") as active_to
      , asa.*
    FROM `fulfillment-dwh-production.curated_data_shared.vendor_asa_configuration_versions`
    LEFT JOIN UNNEST(dps_asa_configuration_history) asa
  )


  , load_orders AS (
    SELECT orders.entity_id 
    , platform_order_code
    , order_placed_at
    , created_date_local
    , customer_location
    , vertical_type
    , IFNULL(customer_total_orders,0) AS customer_total_orders
    , IFNULL(customer_total_orders_darkstores,0) AS customer_total_orders_darkstores
    , IFNULL(customer_total_orders_quick_commerce,0) AS customer_total_orders_quick_commerce
    , SAFE_DIVIDE(IFNULL(DATETIME_DIFF(order_placed_at, customer_first_order_date, SECOND), 0)
      , 86400) AS days_since_first_order
    , SAFE_DIVIDE(IFNULL(DATETIME_DIFF(order_placed_at, customer_first_order_darkstores_date, SECOND), 0)
      , 86400) AS days_since_first_darkstores_order
    , SAFE_DIVIDE(IFNULL(DATETIME_DIFF(order_placed_at, customer_first_order_quick_commerce_date, SECOND), 0)
      , 86400) AS days_since_first_qcommerce_order
    , scheme_id as order_scheme_id
    , assignment_id as order_assignment_id
    , vendor_price_scheme_type
    , partial_assignments 
    , dps_basket_value
    , dps_travel_time
    , dps_travel_time_fee_local
    , dps_standard_fee_local
    , components
    , asa_config.* EXCEPT(entity_id, vendor_code, active_from, active_to)
    , has_time_condition as order_has_time_condition
    , conditions
    
    FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` orders
    INNER JOIN load_asa_config asa_config
      ON orders.entity_id = asa_config.entity_id
      AND orders.vendor_id = asa_config.vendor_code
      AND orders.order_placed_at >= asa_config.active_from
      AND orders.order_placed_at < asa_config.active_to
    WHERE created_date >= "2023-09-01"
    AND created_date_local >= "2023-10-01"
    AND is_own_delivery
    AND is_sent
    -- AND vendor_price_scheme_type in ("Automatic scheme", "Campaign")
    -- AND ( (dps_surge_fee_local IS NULL) OR (dps_surge_fee_local = 0) )
    -- AND has_customer_area_condition = FALSE 
    -- AND has_new_customer_condition = FALSE
  )

  , add_conditions_checks AS (
    SELECT * EXCEPT(asa_price_config)
    , ARRAY(
            SELECT STRUCT(
            scheme_id
            , priority
            , scheme_component_ids.travel_time_config_id
            , scheme_price_mechanisms.is_fleet_delay
            , scheme_component_configs
            , schedule_id
            , schedule_config
            , customer_condition_id
            , ARRAY_TO_STRING(ARRAY(SELECT CAST(x.area_id AS STRING) FROM UNNEST(area_configs) x), ",") as area_ids

            , IF(
              schedule_id IS NULL, TRUE, 
              CHECK_TIME_CONDITIONS(
                order_placed_at
                , schedule_config.start_at
                , schedule_config.end_at
                , schedule_config.recurrence
                , schedule_config.recurrence_end_at
                , schedule_config.condition_end_at
                , schedule_config.active_days
                , schedule_config.is_all_day
              )
            ) AS has_time_condition

            , IF( customer_condition_id IS NULL, TRUE
              , CHECK_CUSTOMER_CONDITION(
                  vertical_type 
                  , is_default_scheme
                  , customer_condition_config
                  , customer_total_orders
                  , customer_total_orders_darkstores 
                  , customer_total_orders_quick_commerce 
                  , days_since_first_order
                  , days_since_first_darkstores_order
                  , days_since_first_qcommerce_order
              )
            ) AS has_new_customer_condition
            , IF( ARRAY_LENGTH(area_configs) = 0, TRUE
              , EXISTS(
                SELECT x 
                FROM UNNEST(area_configs) x 
                WHERE ST_CONTAINS(x.polygon, customer_location)
              ) 
            ) AS has_customer_location_condition
          )
          FROM UNNEST(asa_price_config) apc 
    ) AS price_config
    FROM load_orders
    WHERE TRUE
  )

  , fetch_applied_scheme AS (
    SELECT * EXCEPT(order_scheme_id, price_config)
    , order_scheme_id
    , (SELECT 
        x 
      FROM UNNEST(price_config) x
      WHERE TRUE
      AND x.has_time_condition
      AND x.has_new_customer_condition
      AND x.has_customer_location_condition
      ORDER BY priority
      LIMIT 1
    ) as applied_config 
    FROM add_conditions_checks
  )



  SELECT *
  , IF(
    applied_config IS NULL, NULL,
    (SELECT x.travel_time_fee
      FROM UNNEST(applied_config.scheme_component_configs.travel_time_config) x
      -- WITH OFFSET as tier
      WHERE dps_travel_time <= IFNULL(x.travel_time_threshold, 99999)
      -- ORDER BY tier
      LIMIT 1
    )
  ) AS asa_dps_delivery_fee
  FROM fetch_applied_scheme
  WHERE TRUE

######################