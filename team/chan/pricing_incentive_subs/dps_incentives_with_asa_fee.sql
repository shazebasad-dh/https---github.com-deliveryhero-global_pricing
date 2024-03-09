/*

What's considered discount a DPS discount? 

Any order where
- Orders made by subscription users who got delivery fee benefits
- Orders made with campaign assignments
- Orders made with customer conditions

IF an order has a dps discount, the ideal situation would be:

- dps_delivery_fee_local is always the value inclusive of discount, hence it's called incentivized delivery fee
- total_incentives_local (which is a combination of dps_incentive_local/dps_basket_discount) always logs the correct incentive amount given (including assignments that would have apply and all conditions)
- the "base delivery fee", the fee before incentives can be retrieved adding up the dps_delivery_fee_local and total_incentives_total.

Now, these are the few cases where this does not apply. It generally happens that total_incentives_local IS NULL. 
Therefore, we cannot get the base delivery fee as in the ideal situation. What to do? 

We assume that the base delivery fee is equal to IFNULL(dps_standard_fee_local, asa_dps_delivery_fee). 

- dps_standard_fee_local represents the dps_travel_time_fee_local that would have applied had the incentive not applied. It's main purpose is to be used as strikethrough price for communication to the end-user. It's come
directly from DPS logs. 

- asa_dps_delivery_fee is similar to the dps_standard_fee_local in what it represents. The key difference is that it comes from simulating the DPS logic leveraging the tables with vendor price configurations at our disposal and
estimating the fee that would have applied. 

Since dps_standard_fee_local comes directly from the data producedr, it's considered of HIGHER quality until proven otherwise. This explains the use of IFNULL(dps_standard_fee_local, asa_dps_delivery_fee). However,
both values have a weakness, they both omit any dps_surge_fee_local in case it would have applied. It needs to be assessed how to retrieve this information to improve the accuracy of base_delivery_fee estimation.

Once we estimate base_delivery_fee, the incentive amount is simply the difference between the base_delivery_fee and dps_delivery_fee_local


Why the incentive amount would be null? there are two reason:
- The base_delivery_fee is equal or lower than the incentivized delivery fee. Any time the incentive is 0 or negative, it will be NULL (as confirmed by the DPS API team).
- Logging problems, we can identify/quantify them if the base_delivery_fee (after estimating it) is still higher than dps_delivery_fee but DPS failed to register a discount. 

*/


########################################## INPUTS

  ########## DECLARE VARIABLES

  DECLARE from_date_filter, to_date_filter DATE;
  DECLARE backfill BOOL;


  ########## SET RUN MODE
  SET backfill = FALSE;

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

########################################## 

########################################## CREATE ORDER TABLE

  CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.pricing_incentive_data` AS
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
        , has_subscription_discount

      --- Incentive related field
        /*
        Ideally, there should not be "Other" but we would rather
        catch unexpected system/data producer behavior than ignoring them. 
        It also higlights improvement to our curation logic such as the
        ongoing thread here https://deliveryhero.slack.com/archives/C01903CCRU7/p1708094671467919
        */
        , CASE
          WHEN has_subscription_discount THEN TRUE
          WHEN vendor_price_scheme_type = "Campaign" THEN TRUE
          WHEN has_new_customer_condition THEN TRUE
          WHEN ABS(dps_incentive_discount_local) > 0  THEN TRUE
          ELSE FALSE
        END AS has_dps_discount
        , CASE
          WHEN has_subscription_discount THEN "Subscription"
          WHEN vendor_price_scheme_type = "Campaign" THEN "Campaign"
          WHEN has_new_customer_condition THEN "FDNC"
          WHEN ABS(dps_incentive_discount_local) > 0  THEN "Other"
          ELSE NULL
        END AS incentive_type
        /*
        -- this logs discount amount only if base delivery fee is higher than incentivized delivery fee
        -- values from source are negative, take the absolute value to imply that positive values are in line
        with the expected scenario and control if behavior changes in the future.
        */
        , ABS(dps_incentive_discount_local) AS dps_incentive_discount_local
        , vendor_price_scheme_type
        , has_new_customer_condition
      ---- Rest
        , dps_travel_time_fee_local
        , dps_surge_fee_local
        , dps_basket_value_fee_local
        , dps_delivery_fee_local
        , delivery_fee_local
        , exchange_rate
        , partial_assignments 
        , dps_basket_value
        , dps_travel_time
        /*
        -- fee used for COMMUNICATION purposes (Strikethrough Prices), it's not exactly the delivery fee before incentive as it only cover the
        distance base delivery fee and ignore surge pricing but distance based is usually the biggest contributor to the final delivery fee.
        */
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

  , add_asa_delivery_fee AS (

    SELECT 
      entity_id
      , platform_order_code
      , created_date_local
      , order_placed_at_local
      , dps_travel_time_fee_local
      , dps_surge_fee_local
      , dps_basket_value_fee_local
      , dps_delivery_fee_local
      , delivery_fee_local
      , dps_standard_fee_local
      , exchange_rate
      , vertical_type
      , dps_session_id
      , vendor_price_scheme_type
      , has_dps_discount
      , dps_incentive_discount_local
      , incentive_type
      , IF(
        applied_asa_config IS NULL, NULL,
        (SELECT x.travel_time_fee
          FROM UNNEST(applied_asa_config.travel_time_config) x
          WHERE dps_travel_time <= IFNULL(x.travel_time_threshold, 99999)
          LIMIT 1 --source array is already ordered by ascending tier
        )
      ) AS asa_dps_delivery_fee
    FROM fetch_applied_scheme
    WHERE TRUE
  )


##########################################

  SELECT * EXCEPT(dps_incentive_discount_local)
  /*
  When there is a DPS discount, dps_delivery_fee_local is the incentivized delivery fee. 
  Add the incentive amount to get the base delivery fee. 
  In case there's discount but no incentive amount we use standard fee or the calculated delivery fee
  */
  , CASE
      WHEN has_dps_discount AND dps_incentive_discount_local IS NOT NULL THEN dps_delivery_fee_local + dps_incentive_discount_local
      WHEN has_dps_discount THEN IFNULL(dps_standard_fee_local, asa_dps_delivery_fee)
      ELSE dps_delivery_fee_local
    END AS base_delivery_fee_local

  /*
  Add to the incentive field the cases
  where the base delivery fee could be lower than dps_delivery_fee local which is equal
  to the incentivized delivery fee if there is a dps discount
  */
  , CASE
      WHEN has_dps_discount AND dps_incentive_discount_local IS NOT NULL THEN dps_incentive_discount_local
      WHEN has_dps_discount THEN IFNULL(dps_standard_fee_local, asa_dps_delivery_fee) - dps_delivery_fee_local
      ELSE NULL
    END AS dps_incentive_discount_local
  FROM add_asa_delivery_fee
  WHERE TRUE 