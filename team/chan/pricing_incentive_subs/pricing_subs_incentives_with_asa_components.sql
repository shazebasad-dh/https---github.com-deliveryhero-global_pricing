########################################## INPUTS

  ########## DECLARE VARIABLES

  DECLARE from_date_filter, to_date_filter DATE;
  DECLARE backfill BOOL;


  ########## SET RUN MODE
  SET backfill = TRUE;

  # SET END DATE
  SET to_date_filter = CURRENT_DATE();

  # SET PARTITION DATE
  IF backfill THEN
  SET from_date_filter = DATE_SUB("2023-01-01", interval 0 DAY);
  ELSE
  SET from_date_filter = DATE_TRUNC(DATE_SUB(to_date_filter, interval 1 MONTH), MONTH);
  END IF;
##########################################


##########################################  FUNCTIONS 

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

  ###################### DPS INCENTIVE ID
    CREATE TEMP FUNCTION GENERATE_DPS_INCENTIVE_ID(assignment_type STRING, assignment_id NUMERIC)
    RETURNS STRING
    AS (
    CONCAT("dynamic-pricing:", ARRAY_TO_STRING(SPLIT(LOWER(assignment_type), " "), "-"), ":", CAST(assignment_id AS STRING))
    );
  ######################

########################################## 

########################################## CREATE ORDER TABLE

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.pricing_subs_incentive_data` AS

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
      , order_placed_at_local
      --- Needed to get vendor_group_price_config
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
        , asa_config.* EXCEPT(entity_id, vendor_code, active_from, active_to)
        , dps_session_id
        , has_subscription


      --- Incentive related field
        , CASE
          WHEN ABS(dps_incentive_discount_local) > 0  THEN TRUE
          WHEN has_subscription_discount THEN TRUE
          WHEN vendor_price_scheme_type = "Campaign" THEN TRUE
          WHEN has_new_customer_condition THEN TRUE
          ELSE FALSE
        END AS has_dps_discount

        , has_subscription_discount

        , CASE
          WHEN has_subscription_discount THEN ABS(dps_basket_value_fee_local)
          ELSE ABS(dps_incentive_discount_local)
        END AS total_incentives_local

        , vendor_price_scheme_type
        , has_new_customer_condition

      ---- Rest
        , dps_travel_time_fee_local
        , dps_surge_fee_local
          ----- basket value discount are not discount except for subs. I want this column to reflect non-discount basket value discounts. 
        , IF(has_subscription_discount, 0, dps_basket_value_fee_local) AS dps_basket_value_fee_local
        , IF(has_subscription_discount, dps_delivery_fee_local + ABS(dps_basket_value_fee_local), dps_delivery_fee_local) as base_delivery_fee
        , dps_delivery_fee_local
        , delivery_fee_local
        , exchange_rate

        , partial_assignments 
        , dps_basket_value
        , dps_travel_time
        , IFNULL(dps_mean_delay, mean_delay) as mean_delay
        , dps_standard_fee_local
    
    FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` orders
    INNER JOIN load_asa_config asa_config
      ON orders.entity_id = asa_config.entity_id
      AND orders.vendor_id = asa_config.vendor_code
      AND orders.order_placed_at >= asa_config.active_from
      AND orders.order_placed_at < asa_config.active_to
    WHERE created_date BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
      AND is_sent
      AND is_own_delivery
      AND dps_delivery_fee_local is not null
      AND created_date_local BETWEEN from_date_filter AND to_date_filter
  )

  , fetch_applied_scheme AS (
    SELECT * EXCEPT(asa_price_config)
    , (
            SELECT STRUCT(
            scheme_id
            , priority
            , asa_price_config_id
            , scheme_component_configs.travel_time_config
            )
            
            FROM UNNEST(asa_price_config) apc 
            WHERE TRUE
            AND IF(
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
            ) 

            AND IF( customer_condition_id IS NULL, TRUE
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
            ) 

            AND IF( ARRAY_LENGTH(area_configs) = 0, TRUE
              , EXISTS(
                SELECT x 
                FROM UNNEST(area_configs) x 
                WHERE ST_CONTAINS(x.polygon, customer_location)
              )
            )

            ORDER BY priority
            LIMIT 1
    ) AS applied_asa_config
    FROM load_orders
    WHERE TRUE
  )

  , generate_dps_incentive_array AS (

    SELECT 
      entity_id
      , platform_order_code
      , created_date_local
      , order_placed_at_local
      , dps_travel_time_fee_local
      , dps_surge_fee_local
      , dps_basket_value_fee_local
      , dps_delivery_fee_local
      -- ignores surge fee atm
      /*
      dps_delivery_fee is after-discount applied.
      In case of discount, sum again the value.
      In case the discount is NULL (e.g, the base fee is lower
      than paid, use either)
      */
      -- , CASE
      --     WHEN has_dps_discount 
      --       THEN
      --       dps_delivery_fee 
      --       + IFNULL(
      --         total_incentives_local,
      --         IFNULL(
      --           dps_standard_fee_local
      --           , (SELECT x.travel_time_fee
      --             FROM UNNEST(applied_asa_config.travel_time_config) x
      --               WHERE dps_travel_time <= IFNULL(x.travel_time_threshold, 99999)
      --               LIMIT 1
      --             )
      --         ) 
      --       )
      --     ELSE dps_delivery_fee
      --   END base_delivery_fee
      , delivery_fee_local
      , dps_standard_fee_local
      , exchange_rate
      , vertical_type
      , has_subscription
      , dps_session_id
      , vendor_price_scheme_type
      , has_dps_discount
      , IF(
        applied_asa_config IS NULL, NULL,
        (SELECT x.travel_time_fee
          FROM UNNEST(applied_asa_config.travel_time_config) x
          -- WITH OFFSET as tier
          WHERE dps_travel_time <= IFNULL(x.travel_time_threshold, 99999)
          -- ORDER BY tier
          LIMIT 1
        )
      ) AS asa_dps_delivery_fee
      , CASE 
          WHEN ANY_VALUE(has_dps_discount) THEN 
          ARRAY_AGG(
            STRUCT(

              --- ignoring manual/country fallback and null assignment types as they represent, 0.03% of orders
            CASE
              WHEN has_subscription_discount AND vendor_price_scheme_type = "Automatic scheme" THEN GENERATE_DPS_INCENTIVE_ID("subscription", applied_asa_config.asa_price_config_id)
              WHEN has_subscription_discount AND vendor_price_scheme_type = "Experiment" THEN GENERATE_DPS_INCENTIVE_ID("subscription", order_assignment_id)
              WHEN vendor_price_scheme_type = "Campaign" THEN GENERATE_DPS_INCENTIVE_ID(vendor_price_scheme_type, order_assignment_id)
              WHEN vendor_price_scheme_type = "Automatic scheme" THEN GENERATE_DPS_INCENTIVE_ID("Automatic assignment", applied_asa_config.asa_price_config_id)
              ELSE NULL
            END AS customer_incentive_id

            , "discount" AS incentive_level_category
            , "delivery_fee" AS incentive_level_type
            , "DPS" as channel_subchannel
            , IF(has_subscription_discount, TRUE, FALSE) as is_membership_only

            , total_incentives_local
            -- assume all is on DH until solution is found
            , total_incentives_local AS dh_contribution_local
            , 0 as vendor_contribution_local
            )
          )
        END as dps_incentive
    FROM fetch_applied_scheme
    WHERE TRUE
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  )

  , load_bima_incentives AS (
    SELECT
      order_id
      , global_entity_id
      , ARRAY(
          SELECT
            STRUCT(x.customer_incentive_id
            , x.incentive_level_category
            , x.incentive_level_type
            , x.channel_subchannel
            , x.is_membership_only
            , x.total_incentives_lc
            , x.dh_contribution_lc 
            , x.vendor_contribution_lc
            )
          FROM UNNEST(incentive_spend) x
          WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
            AND (
              LOWER(x.incentive_level_type) LIKE "%delivery%"
              OR ( x.deliveryfee_inc_lc > 0 OR  x.total_incentives_lc > 0)
            )
      ) AS bima_incentive_spend
      
      , EXISTS(
          SELECT x
          FROM UNNEST(incentive_spend) x
          WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
      ) AS bima_has_dps_incentive

    FROM `fulfillment-dwh-production.curated_data_shared_mkt.bima_incentives_reporting`
    WHERE created_date BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    --- only delivery fee incentives
    AND EXISTS(
      SELECT x FROM UNNEST(incentive_spend) x
      WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
        AND (
          LOWER(x.incentive_level_type) LIKE "%delivery%"
          OR ( x.deliveryfee_inc_lc > 0 OR  x.total_incentives_lc > 0)
        )
    )
  )

  , load_subs_data AS (
    SELECT
      global_entity_id
      , order_id
      , ARRAY(
        SELECT
          STRUCT(
              x.customer_incentive_id
              , NULL as incentive_level_category
              , "delivery_fee" as incentive_level_type 
              , NULL AS channel_subchannel
              , TRUE AS is_membership_only
              , x.total_value_local
              , x.dh_funded_value_local
              , x.vendor_funded_value_local
          )
        FROM UNNEST(benefits) x
        WHERE LOWER(x.category) LIKE "%delivery%"
      ) as subs_benefits
    FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.subscription_benefit_transactions`
    WHERE DATE(transaction_timestamp_local) BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
  )

  , join_all AS (
    SELECT
    dps.* EXCEPT(has_subscription)
    , bima.* EXCEPT(global_entity_id, order_id, bima_has_dps_incentive)
    , IFNULL(bima_has_dps_incentive, FALSE) as bima_has_dps_incentive
    , subs.* EXCEPT(global_entity_id, order_id)
    , GREATEST(has_subscription, subs.order_id IS NOT NULL) AS has_subscription
    , CASE
        WHEN ARRAY_LENGTH(dps_incentive) > 0  THEN TRUE
        WHEN bima.order_id IS NOT NULL THEN TRUE
        WHEN ARRAY_LENGTH(subs_benefits) > 0 THEN TRUE
        ELSE FALSE
      END AS has_delivery_fee_incentive

    FROM generate_dps_incentive_array dps

    LEFT JOIN load_bima_incentives bima
      ON dps.entity_id = bima.global_entity_id
      AND dps.platform_order_code = bima.order_id

    LEFT JOIN load_subs_data subs
      ON dps.entity_id = subs.global_entity_id
      AND dps.platform_order_code = subs.order_id
  )



##########################################

  SELECT *
  FROM join_all
  WHERE TRUE 